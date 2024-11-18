import socket
import threading
import struct
import time
from queue import Queue
from collections import deque

EXPIRATION_TIME = 120

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class Header:
    HEADER_SIZE = 16
    FIN_FLAG = 1

    @staticmethod
    def create(sequence_number, ack_number, data_size, flags=0):
        return struct.pack('!IIII', sequence_number, ack_number, data_size, flags)

    @staticmethod
    def parse(header_bytes):
        return struct.unpack('!IIII', header_bytes)

class ByteStore:
    def __init__(self):
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.data_available = threading.Condition(self.lock)

    def add(self, data):
        # print("add", len(data))
        with self.data_available:
            self.buffer.extend(data)
            self.data_available.notify_all()

    def get(self, n):
        with self.data_available:
            self.data_available.wait_for(lambda: len(self.buffer) >= n)
            result = self.buffer[:n]
            self.buffer = self.buffer[n:]
            return bytes(result)

class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_lock = threading.Lock()
        self.running = 0
        self.readed_bytes = 0
        self.unacked_packets = {}
        self.sended_bytes = 0
        self.delivered_bytes = 0
        self.received_data = ByteStore()
        self.send_lock = threading.Lock()

        self.reader_thread = threading.Thread(target=self.read_process)
        self.writer_thread = threading.Thread(target=self.write_process)
        self.reader_thread.start()
        self.writer_thread.start()

    def send_ack(self):
        self.send_impl(bytes(), self.sended_bytes)

    def finished(self):
        # print(self, self.running)
        with self.state_lock:
            # print(self.running)
            return self.running == 3 and self.sended_bytes == self.delivered_bytes  # Equivalent to RunningState::Finished

    def recv_fin(self):
        with self.state_lock:
            self.running |= 1  # Equivalent to RunningState::RecvFin

    def send_fin(self):
        with self.state_lock:
            self.running |= 2  # Equivalent to RunningState::SendFin

    def filter_unacked(self):
        with self.send_lock:
            self.unacked_packets = {seq_num: packet for seq_num, packet in self.unacked_packets.items() if seq_num >= self.delivered_bytes or packet[2] != 0}

    def read_process(self):
        while not self.finished():
            try:
                msg = self.recvfrom(1500)
                seq_number, recv_ack_number, data_size, flags = Header.parse(msg[:Header.HEADER_SIZE])
                data = bytes(msg[Header.HEADER_SIZE:])

                if data_size != len(data):
                    raise Exception("Invalid size")

                self.delivered_bytes = max(self.delivered_bytes, recv_ack_number)
                self.filter_unacked()

                if flags & Header.FIN_FLAG:
                    self.recv_fin()
                    self.close_impl()

                if seq_number == self.readed_bytes:
                    self.received_data.add(data)
                    self.readed_bytes += data_size
                if data_size != 0:
                    self.send_ack()

            except Exception as e:
                print("Read error:", e)
                return

    def write_process(self):
        while not self.finished():
            time.sleep(0.01)
            send_queue = []
            with self.send_lock:
                current_time = int(time.time() * 1000)
                for seq_num, (data, send_time, flags) in list(self.unacked_packets.items()):
                    if current_time - send_time > EXPIRATION_TIME:
                        send_queue.append((data, seq_num, flags))

            send_queue = sorted(send_queue, key=lambda x: x[1])[:3]
            for (data, seq_num, flags) in send_queue:
                self.send_impl(data, seq_num, flags)
            self.send_ack()

    def add_unacked_packets(self, data, flags=0):
        current_time = int(time.time() * 1000)
        self.unacked_packets[self.sended_bytes] = (data, current_time, flags)
        self.sended_bytes += len(data)

    def send_impl(self, data, seq_num, flags=0):
        header = Header.create(seq_num, self.readed_bytes, len(data), flags)
        packet = header + data
        self.sendto(packet)

    def send(self, data: bytes):
        max_packet_size = 1400
        with self.send_lock:
            for i in range(0, len(data), max_packet_size):
                part_data = data[i:i + max_packet_size]
                self.send_impl(part_data, self.sended_bytes)
                self.send_impl(part_data, self.sended_bytes)
                self.send_impl(part_data, self.sended_bytes)
                self.add_unacked_packets(part_data)
        return len(data)

    def recv(self, n: int):
        return self.received_data.get(n)

    def close_impl(self):
        if self.running & 2 == 0:
            with self.send_lock:
                self.send_impl(b'x', self.sended_bytes, Header.FIN_FLAG)
                self.add_unacked_packets(b'x', Header.FIN_FLAG)

            self.send_fin()

    def close(self):
        self.close_impl()

        self.reader_thread.join()
        self.writer_thread.join()
        super().close()
