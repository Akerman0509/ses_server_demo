#!/usr/bin/env python3
"""
Launcher for SES Distributed System
Khởi động 15 processes đồng thời
"""

import subprocess
import sys
import time
import os
import signal
import json
from pathlib import Path

class ProcessLauncher:
    def __init__(self, config_path="config/config.json"):
        self.config_path = config_path
        self.processes = []
        self.load_config()
        
    def load_config(self):
        """Load configuration từ file"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            print(f"✓ Loaded configuration: {self.config['num_processes']} processes")
        except FileNotFoundError:
            print(f"✗ Config file not found: {self.config_path}")
            print("Creating default config...")
            self.create_default_config()
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
    
    def create_default_config(self):
        """Tạo file config mặc định"""
        os.makedirs('config', exist_ok=True)
        
        default_config = {
            "num_processes": 15,
            "messages_per_process": 150,
            "message_rate": {
                "min_per_minute": 10,
                "max_per_minute": 100
            },
            "network": {
                "base_port": 5000,
                "timeout": 30
            },
            "logging": {
                "level": "INFO",
                "format": "[%(asctime)s] [%(levelname)s] %(message)s"
            },
            "processes": []
        }
        
        # Tạo danh sách processes
        for i in range(default_config["num_processes"]):
            default_config["processes"].append({
                "id": i,
                "host": "localhost",
                "port": default_config["network"]["base_port"] + i
            })
        
        with open(self.config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"✓ Created default config at {self.config_path}")
    
    def setup_environment(self):
        """Chuẩn bị môi trường: tạo thư mục logs"""
        os.makedirs('logs', exist_ok=True)
        print("✓ Created logs directory")
        
        # Xóa các log files cũ
        for log_file in Path('logs').glob('process_*.log'):
            log_file.unlink()
        print("✓ Cleaned old log files")
    
    def launch_process(self, process_id):
        """Khởi động một process"""
        try:
            # Chạy process trong subprocess
            process = subprocess.Popen(
                [sys.executable, 'single_process.py', str(process_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.processes.append({
                'id': process_id,
                'process': process
            })
            print(f"✓ Launched Process {process_id} (PID: {process.pid})")
            return True
        except Exception as e:
            print(f"✗ Failed to launch Process {process_id}: {e}")
            return False
    
    def launch_all(self):
        """Khởi động tất cả processes"""
        print("\n" + "="*60)
        print("  SES DISTRIBUTED SYSTEM LAUNCHER")
        print("="*60)
        print(f"\nLaunching {self.config['num_processes']} processes...\n")
        
        self.setup_environment()
        
        # Khởi động từng process với delay nhỏ
        success_count = 0
        for i in range(self.config['num_processes']):
            if self.launch_process(i):
                success_count += 1
            time.sleep(0.2)  # Delay nhỏ để tránh race condition
        
        print(f"\n{'='*60}")
        print(f"✓ Successfully launched {success_count}/{self.config['num_processes']} processes")
        print(f"{'='*60}\n")
        
        if success_count < self.config['num_processes']:
            print("⚠ Warning: Not all processes started successfully!")
        
        return success_count == self.config['num_processes']
    
    def monitor_processes(self):
        """Giám sát các processes"""
        print("\nMonitoring processes... Press Ctrl+C to stop all.\n")
        print(f"{'Process ID':<12} {'PID':<8} {'Status':<10}")
        print("-" * 60)
        
        try:
            while True:
                time.sleep(2)
                
                # Kiểm tra trạng thái các processes
                for p_info in self.processes:
                    pid = p_info['process'].pid
                    status = p_info['process'].poll()
                    
                    if status is None:
                        status_str = "Running"
                    else:
                        status_str = f"Exited({status})"
                    
                    print(f"\rProcess {p_info['id']:<6} {pid:<8} {status_str:<10}", end='')
                
                # Kiểm tra nếu tất cả processes đã kết thúc
                all_finished = all(
                    p['process'].poll() is not None 
                    for p in self.processes
                )
                
                if all_finished:
                    print("\n\n✓ All processes have finished.")
                    break
                    
        except KeyboardInterrupt:
            print("\n\n⚠ Received interrupt signal. Shutting down...")
            self.shutdown_all()
    
    def shutdown_all(self):
        """Tắt tất cả processes"""
        print("\nShutting down all processes...")
        
        for p_info in self.processes:
            try:
                process = p_info['process']
                if process.poll() is None:  # Nếu process vẫn đang chạy
                    process.terminate()
                    print(f"✓ Terminated Process {p_info['id']} (PID: {process.pid})")
                    
                    # Đợi process kết thúc
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print(f"⚠ Force killing Process {p_info['id']}")
                        process.kill()
            except Exception as e:
                print(f"✗ Error shutting down Process {p_info['id']}: {e}")
        
        print("\n✓ All processes stopped.")
    
    def show_logs(self):
        """Hiển thị thông tin về log files"""
        print("\n" + "="*60)
        print("LOG FILES")
        print("="*60)
        
        log_files = sorted(Path('logs').glob('process_*.log'))
        
        if not log_files:
            print("No log files found.")
            return
        
        for log_file in log_files:
            size = log_file.stat().st_size
            print(f"{log_file.name:<20} {size:>10} bytes")
        
        print(f"\nTotal log files: {len(log_files)}")
        print("="*60)

def main():
    """Main function"""
    print("\n" + "="*60)
    print("  SES DISTRIBUTED SYSTEM")
    print("  Schiper-Eggli-Sandoz Causal Ordering Algorithm")
    print("="*60)
    
    launcher = ProcessLauncher()
    
    # Khởi động tất cả processes
    if launcher.launch_all():
        # Giám sát processes
        launcher.monitor_processes()
        
        # Hiển thị thông tin log
        launcher.show_logs()
    else:
        print("\n✗ Failed to launch all processes. Check the errors above.")
        launcher.shutdown_all()
        return 1
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)