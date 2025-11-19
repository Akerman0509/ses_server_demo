#!/usr/bin/env python3
"""
Single Process for SES Distributed System
Mỗi process chạy độc lập, gửi và nhận messages
"""

import socket
import threading
import time
import json
import sys
import logging
import random
from datetime import datetime
from pathlib import Path

class MsgQueue:
    def __init__ (self, num_processes):
        self.queue = [0] * num_processes
        self.q_length = num_processes
        
        
    def update_queue_recv(self, pid, other_queue):
        for i in range(self.q_length):
            if i != pid:
                self.queue[i] = self.vector_merge(self.queue[i], other_queue[i])

        
    def update_queue_send(self, target_pid, vector_clock):
        temp = self.vector_merge(self.queue[target_pid], vector_clock)
        self.queue[target_pid] = temp
    
    @staticmethod
    def vector_merge(v1, v2):
        if v1 == 0:
            return v2
        elif v2 == 0:
            return v1
        return [max(a, b) for a, b in zip(v1, v2)]
        
                
    
class VectorClock:
    """Vector Clock implementation"""
    def __init__(self, num_processes, process_id):
        self.vector = [0] * num_processes
        self.process_id = process_id
        self.num_processes = num_processes
        self.msg_queue = MsgQueue(num_processes)
    
    def increment(self):
        """Tăng clock của process hiện tại"""
        self.vector[self.process_id] += 1
    
    def update(self, msg):
        """Cập nhật clock khi nhận message (element-wise max)"""
        for i in range(self.num_processes):
            if i != self.process_id:
                self.vector[i] = max(self.vector[i], msg.timestamp[i])
        # Tăng clock của chính mình
        self.vector[self.process_id] += 1
        
        # Cập nhật msg queue
        self.msg_queue.update_queue_recv(self.process_id, msg.msg_queue)
        
    
    def vector_clock_copy(self):
        """Trả về bản copy của vector"""
        return self.vector.copy()
    def msg_queue_copy(self):   
        return self.msg_queue.queue.copy()
    
    def __str__(self):
        return str(self.vector)

class Message:
    """Message class với timestamp"""
    def __init__(self, sender_id, receiver_id, content, timestamp,msg_queue,msg_number):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.content = content
        self.timestamp = timestamp  # Vector clock
        self.msg_queue = msg_queue
        
        self.msg_number = msg_number
        self.arrival_time = datetime.now()
        
    
    def to_dict(self):
        """Convert message to dictionary for JSON serialization"""
        return {
            'sender_id': self.sender_id,
            'receiver_id': self.receiver_id,
            'content': self.content,
            'timestamp': self.timestamp,
            'msg_queue': self.msg_queue,
            'msg_number': self.msg_number
        }
    
    @staticmethod
    def from_dict(data):
        """Create message from dictionary"""
        msg = Message(
            data['sender_id'],
            data['receiver_id'],
            data['content'],
            data['timestamp'],
            data['msg_queue'],
            data['msg_number']
        )
        return msg
    
    def __str__(self):
        return f"Msg#{self.msg_number} P{self.sender_id} → P{self.receiver_id}"

class Process:
    """Main Process class implementing SES algorithm"""
    def __init__(self, process_id, config):
        self.process_id = process_id
        self.config = config
        self.num_processes = config['num_processes']
        
        # Vector clock
        self.vector_clock = VectorClock(self.num_processes, process_id)
        self.clock_lock = threading.Lock()
        
        # Message management
        self.message_buffer = []  # Messages chờ deliver
        self.buffer_lock = threading.Lock()
        self.delivered_messages = []
        self.delivered_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'received': 0,
            'delivered': 0,
            'buffered': 0
        }
        self.stats_lock = threading.Lock()
        
        # Networking
        self.host = config['processes'][process_id]['host']
        self.port = config['processes'][process_id]['port']
        self.server_socket = None
        
        # Threading
        self.sender_threads = []
        self.receiver_thread = None
        self.running = True
        
        # Logging
        self.setup_logging()
        
        # Message counter for each target process
        self.sent_count = {i: 0 for i in range(self.num_processes) if i != self.process_id}
        
        
    
    def setup_logging(self):
        """Thiết lập logging cho process"""
        log_file = f"logs/process_{self.process_id}.log"
        
        # Tạo logger
        self.logger = logging.getLogger(f"Process{self.process_id}")
        self.logger.setLevel(logging.DEBUG)
        
        # File handler
        fh = logging.FileHandler(log_file, mode='w')
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        
        self.logger.info(f"Process {self.process_id} initialized")
        self.logger.info(f"Vector Clock: {self.vector_clock}")
    
    def start_server(self):
        """Khởi động server socket để nhận messages"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(20)
            self.logger.info(f"Server started on {self.host}:{self.port}")
            
            # Bắt đầu receiver thread
            self.receiver_thread = threading.Thread(target=self.receiver_loop, daemon=True)
            self.receiver_thread.start()
            
        except Exception as e:
            self.logger.error(f"Failed to start server: {e}")
            raise
    
    def receiver_loop(self):
        """Thread nhận messages từ các processes khác"""
        self.logger.info("Receiver thread started")
        
        while self.running:
            try:
                self.server_socket.settimeout(2.0)
                client_socket, addr = self.server_socket.accept()
                
                # Xử lý connection trong thread riêng
                handler_thread = threading.Thread(
                    target=self.handle_connection,
                    args=(client_socket,),
                    daemon=True
                )
                handler_thread.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.error(f"Receiver error: {e}")
    
    def handle_connection(self, client_socket):
        """Xử lý một connection từ sender"""
        # try:
            # Nhận data
        data = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(data) > 1024 * 1024:  # Max 1MB
                break
        
        if data:
            msg_dict = json.loads(data.decode('utf-8'))
            msg = Message.from_dict(msg_dict)
            
            # Xử lý message
            client_socket.sendall(b"ACK")
            self.receive_message(msg)
                
        # except Exception as e:
        #     self.logger.error(f"Error handling connection: {e}")
        # finally:
        #     client_socket.close()
    
    def receive_message(self, msg):
        """Xử lý message nhận được"""
        with self.stats_lock:
            self.stats['received'] += 1
        
        self.logger.info(f"[RECEIVED] {msg} with TS={msg.timestamp}, MSG_QUEUE={msg.msg_queue}")
        old_msq_queue = self.vector_clock.msg_queue.queue.copy()
        # Cập nhật msg queue nhận
        self.vector_clock.msg_queue.update_queue_recv(self.process_id, msg.msg_queue)
        self.logger.info(f"MSG_queue update: {old_msq_queue} →  {self.vector_clock.msg_queue.queue}")
        
        
        # Kiểm tra điều kiện delivery
        if self.can_deliver(msg):
            self.deliver_message(msg)
            # Thử deliver các messages trong buffer
            self.try_deliver_buffered()
        else:
            self.buffer_message(msg)
    
    def compare_bigger_equal(self, v1, v2):
        for a, b in zip(v1, v2):
            if a < b:
                return False
        return True
    
    def can_deliver(self, msg):
        sender = msg.sender_id
        ts = msg.timestamp
        msg_queue = msg.msg_queue
        
        with self.clock_lock:
            vc = self.vector_clock.vector
            # compare current clock with msg_queue
            if msg_queue[self.process_id] == 0: 
                return True
            else:
                if not self.compare_bigger_equal(vc, msg_queue[self.process_id]):
                    self.logger.debug(
                        f"Cannot deliver {msg}: VC={vc} < MSG_QUEUE[{self.process_id}]={msg_queue[self.process_id]}"
                    )
                    return False
            return True
    
    def deliver_message(self, msg):
        """Deliver message và cập nhật vector clock"""
        with self.clock_lock:
            old_vc = self.vector_clock.vector_clock_copy()
            self.vector_clock.update(msg)
            new_vc = self.vector_clock.vector_clock_copy()
        
        with self.delivered_lock:
            self.delivered_messages.append(msg)
        
        with self.stats_lock:
            self.stats['delivered'] += 1
        
        self.logger.info(
            f"[✓ DELIVERED] {msg}, VC: {old_vc} → {new_vc}"
        )
    
    def buffer_message(self, msg):
        """Đưa message vào buffer"""
        with self.buffer_lock:
            self.message_buffer.append(msg)
        
        with self.stats_lock:
            self.stats['buffered'] += 1
        
        # Tìm lý do buffer
        sender = msg.sender_id
        ts = msg.timestamp
        with self.clock_lock:
            vc = self.vector_clock.vector
            
            reasons = []
            if vc[sender] != ts[sender] - 1:
                reasons.append(f"VC[{sender}]={vc[sender]} != TS[{sender}]-1={ts[sender]-1}")
            
            for k in range(self.num_processes):
                if k != sender and vc[k] < ts[k]:
                    reasons.append(f"VC[{k}]={vc[k]} < TS[{k}]={ts[k]}")
        
        self.logger.warning(
            f"⏸ BUFFERED {msg}"
        )
    
    def try_deliver_buffered(self):
        """Thử deliver các messages trong buffer"""
        with self.buffer_lock:
            if not self.message_buffer:
                return
            
            # Sắp xếp buffer theo timestamp của sender
            self.message_buffer.sort(key=lambda m: (m.sender_id, m.timestamp[m.sender_id]))
            
            delivered = []
            for msg in self.message_buffer[:]:
                if self.can_deliver(msg):
                    self.deliver_message(msg)
                    delivered.append(msg)
                    self.message_buffer.remove(msg)
            
            if delivered:
                self.logger.info(
                    f"✓ UNBUFFERED and delivered {len(delivered)} message(s), "
                    f"buffer size: {len(self.message_buffer)}"
                )
    
    def send_message(self, target_id, content):
        """Gửi message tới process khác"""
        # Increment vector clock
        with self.clock_lock:
            self.vector_clock.increment()
            timestamp = self.vector_clock.vector_clock_copy()

            
        # Tạo message
        self.sent_count[target_id] += 1
        msg = Message(
            self.process_id,
            target_id,
            content,
            timestamp,
            self.vector_clock.msg_queue_copy(),
            self.sent_count[target_id]
        )
        # update msg queue
        self.vector_clock.msg_queue.update_queue_send(target_id, timestamp)
        
        # Gửi qua socket
        target_info = self.config['processes'][target_id]
        target_host = target_info['host']
        target_port = target_info['port']
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((target_host, target_port))
        
        # Serialize và gửi
        msg_json = json.dumps(msg.to_dict())
        sock.sendall(msg_json.encode('utf-8'))
        sock.shutdown(socket.SHUT_WR) #signal that sending is done
        # Đợi ACK
        ack = sock.recv(1024)
        # print (f"!!!!!!received ack: {ack}")
        if ack != b"ACK":
            self.logger.error(f"No ACK received for message to P{target_id}")
        else:
            self.logger.info(f"[→ SENT] {msg}, TS={timestamp}, queue={msg.msg_queue}")
            
        sock.close()
            
        # except Exception as e:
        #     self.logger.error(f"Failed to send message to P{target_id}: {e}")
        #     return False
    
    def sender_thread_func(self, target_id):
        """Thread gửi messages tới một process cụ thể"""
        messages_to_send = self.config['messages_per_process']
        min_rate = self.config['message_rate_min']
        max_rate = self.config['message_rate_max']
        
        self.logger.info(f"Sender thread started for P{target_id}")
        
        # Đợi một chút để tất cả processes khởi động
        time.sleep(3)
        
        for i in range(messages_to_send):
            if not self.running:
                break
            
            # Gửi message
            content = f"message {i+1}"
            self.send_message(target_id, content)
            
            # Random delay dựa trên message rate
            rate = random.uniform(min_rate, max_rate)
            delay = 60.0 / rate  # Convert messages/minute to seconds
            # time.sleep(delay)
            time.sleep(2)
        
        self.logger.info(f"Sender thread completed for P{target_id}")
    
    def shake_hands(self):
        for target_id in range(self.num_processes):
            """Gửi handshake tới tất cả processes khác"""
            if target_id == self.process_id:
                continue
            
    def shake_hands(self, retry_interval=2.0):
        """
        Liên tục thử kết nối đến tất cả process khác
        retry_interval: thời gian (s) giữa các lần thử
        """
        for target_id in range(self.num_processes):
            if target_id == self.process_id:
                continue

            target_info = self.config['processes'][target_id]
            target_host = target_info['host']
            target_port = target_info['port']

            connected = False
            while not connected:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5.0)
                    sock.connect((target_host, target_port))
                    self.logger.info(f"Handshake successful with P{target_id}")
                    connected = True
                except Exception as e:
                    self.logger.warning(f"Cannot connect to P{target_id}, retrying in {retry_interval}s...")
                    time.sleep(retry_interval)
    
    def start_senders(self):
        
        """Khởi động tất cả sender threads"""
        for target_id in range(self.num_processes):
            if target_id == self.process_id:
                continue
            
            thread = threading.Thread(
                target=self.sender_thread_func,
                args=(target_id,),
                daemon=True
            )
            thread.start()
            self.sender_threads.append(thread)
        
        self.logger.info(f"Started {len(self.sender_threads)} sender threads")
    
    def run(self):
        """Chạy process"""
        try:
            # Khởi động server
            self.start_server()
            
            # Thực hiện handshake với tất cả processes khác
            self.shake_hands()
            
            # Khởi động senders
            self.start_senders()
            
            # Chờ tất cả sender threads hoàn thành
            for thread in self.sender_threads:
                thread.join()
            
            # Đợi thêm một chút để nhận messages còn lại
            self.logger.info("All senders completed, waiting for remaining messages...")
            time.sleep(5)
            
            # In statistics
            # self.print_statistics()
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Error in run: {e}")
        finally:
            self.shutdown()
    
    def print_statistics(self):
        """In thống kê"""
        with self.stats_lock:
            stats = self.stats.copy()
        
        with self.buffer_lock:
            buffer_size = len(self.message_buffer)
        
        self.logger.info("="*60)
        self.logger.info("FINAL STATISTICS")
        self.logger.info("="*60)
        self.logger.info(f"Messages Received:    {stats['received']}")
        self.logger.info(f"Messages Delivered:   {stats['delivered']}")
        self.logger.info(f"Messages Buffered:    {stats['buffered']}")
        self.logger.info(f"Current Buffer Size:  {buffer_size}")
        self.logger.info(f"Final Vector Clock:   {self.vector_clock}")
        self.logger.info("="*60)
    
    def shutdown(self):
        """Tắt process"""
        self.logger.info("Shutting down...")
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
        
        self.logger.info("Process terminated")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python single_process.py <process_id>")
        sys.exit(1)
    
    process_id = int(sys.argv[1])
    
    # Load config
    config_path = "config/config.json"
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    
    # Tạo và chạy process
    process = Process(process_id, config)
    process.run()

if __name__ == "__main__":
    main()