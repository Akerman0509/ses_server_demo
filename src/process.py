



class Process:
    def __init__(self, process_id, config):
        self.process_id = process_id
        self.vector_clock = VectorClock(...)
        self.message_buffer = []  # Buffer cho messages chưa deliver
        self.delivered_messages = []
        self.sender_threads = []  # 14 threads gửi messages
        
    def start(self):
        """Khởi động process, receiver và sender threads"""
        
    def receiver_thread(self):
        """Nhận messages từ socket"""
        
    def sender_thread(self, target_process_id):
        """Gửi 150 messages tới process khác"""
        
    def can_deliver(self, message):
        """Kiểm tra điều kiện SES để deliver message"""
        # VC[j] = TS[j] + 1 (j = sender)
        # VC[k] >= TS[k] for all k != j
        
    def buffer_message(self, message):
        """Đưa message vào buffer"""
        
    def try_deliver_buffered(self):
        """Thử deliver các messages trong buffer"""
        
    def deliver_message(self, message):
        """Deliver message và cập nhật vector clock"""
