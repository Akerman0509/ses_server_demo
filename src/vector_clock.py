

class VectorClock:
    def __init__(self, num_processes, process_id):
        self.vector = [0] * num_processes
        self.process_id = process_id
    
    def increment(self):
        """Tăng clock của process hiện tại"""
        
    def update(self, other_vector):
        """Cập nhật clock khi nhận message"""
        
    def compare(self, other_vector):
        """So sánh để kiểm tra điều kiện nhân quả"""