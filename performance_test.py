#!/usr/bin/env python3
"""
Lab 4 Part 4: Performance Testing
Load testing for the IoT streaming pipeline
"""

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading
from collections import defaultdict
import sys

class PerformanceTester:
    """Performance testing for IoT streaming pipeline"""
    
    def __init__(self):
        self.producer = None
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'total_latency': 0.0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        self.lock = threading.Lock()
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='gzip'
            )
            print("✓ Connected to Kafka broker")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def generate_sensor_data(self):
        """Generate sensor events that match the dashboard schema exactly"""
        sensor_types = {
            "temperature": ("°C", random.uniform(10, 40)),
            "humidity": ("%", random.uniform(20, 90)),
            "pressure": ("Pa", random.uniform(90000, 110000)),
            "light": ("lux", random.uniform(100, 2000)),
            "motion": ("state", random.choice([0, 1]))
        }

        locations = ['Building-A', 'Building-B', 'Building-C', 'Warehouse', 'Lab']

        sensor_id = f"SENSOR-{random.randint(1, 100):03d}"
        sensor_type = random.choice(list(sensor_types.keys()))
        unit, value = sensor_types[sensor_type]

        return {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "location": random.choice(locations),
            "value": round(value, 2),
            "unit": unit,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "quality": random.choice(["good", "fair", "poor"])
        }

    
    def send_message(self):
        """Send a single message and track performance"""
        try:
            data = self.generate_sensor_data()
            start_time = time.time()
            
            future = self.producer.send('iot-sensors', value=data)
            record_metadata = future.get(timeout=10)
            
            latency = (time.time() - start_time) * 1000  # Convert to ms
            
            with self.lock:
                self.stats['messages_sent'] += 1
                self.stats['total_latency'] += latency
                
            return True
            
        except KafkaError as e:
            with self.lock:
                self.stats['messages_failed'] += 1
                self.stats['errors'].append(str(e))
            return False
        except Exception as e:
            with self.lock:
                self.stats['messages_failed'] += 1
                self.stats['errors'].append(str(e))
            return False
    
    def worker_thread(self, messages_per_thread, thread_id):
        """Worker thread to send messages"""
        print(f"  Thread {thread_id}: Starting to send {messages_per_thread} messages")
        
        for i in range(messages_per_thread):
            self.send_message()
            
            # Small delay to avoid overwhelming the system
            if i % 100 == 0:
                time.sleep(0.01)
        
        print(f"  Thread {thread_id}: Completed")
    
    def run_test(self, total_messages=1000, num_threads=4):
        """Run performance test"""
        
        print("\n" + "="*70)
        print("IoT Streaming Pipeline - Performance Test")
        print("="*70)
        print(f"\nTest Configuration:")
        print(f"  Total Messages: {total_messages:,}")
        print(f"  Concurrent Threads: {num_threads}")
        print(f"  Messages per Thread: {total_messages // num_threads:,}")
        print(f"  Target Topic: iot-sensors")
        print("\n" + "="*70)
        
        # Connect to Kafka
        if not self.connect_kafka():
            return
        
        # Calculate messages per thread
        messages_per_thread = total_messages // num_threads
        
        # Start test
        self.stats['start_time'] = time.time()
        print(f"\n🚀 Starting test at {datetime.now().strftime('%H:%M:%S')}\n")
        
        # Create and start threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(
                target=self.worker_thread,
                args=(messages_per_thread, i + 1)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        self.stats['end_time'] = time.time()
        
        # Close producer
        self.producer.flush()
        self.producer.close()
        
        # Print results
        self.print_results()
    
    def print_results(self):
        """Print test results"""
        duration = self.stats['end_time'] - self.stats['start_time']
        avg_latency = (self.stats['total_latency'] / self.stats['messages_sent'] 
                      if self.stats['messages_sent'] > 0 else 0)
        throughput = self.stats['messages_sent'] / duration if duration > 0 else 0
        success_rate = (self.stats['messages_sent'] / 
                       (self.stats['messages_sent'] + self.stats['messages_failed']) * 100
                       if (self.stats['messages_sent'] + self.stats['messages_failed']) > 0 else 0)
        
        print("\n" + "="*70)
        print("Performance Test Results")
        print("="*70)
        print(f"\n📊 Message Statistics:")
        print(f"  Total Messages Sent:     {self.stats['messages_sent']:,}")
        print(f"  Failed Messages:         {self.stats['messages_failed']:,}")
        print(f"  Success Rate:            {success_rate:.2f}%")
        
        print(f"\n⏱️  Timing Metrics:")
        print(f"  Total Duration:          {duration:.2f} seconds")
        print(f"  Average Latency:         {avg_latency:.2f} ms")
        print(f"  Throughput:              {throughput:.2f} messages/second")
        
        print(f"\n💻 System Performance:")
        print(f"  Messages per Second:     {throughput:.0f} msg/s")
        print(f"  Messages per Minute:     {throughput * 60:.0f} msg/min")
        print(f"  Messages per Hour:       {throughput * 3600:.0f} msg/hr")
        
        if self.stats['errors']:
            print(f"\n⚠️  Errors Encountered: {len(self.stats['errors'])}")
            print("  Sample errors:")
            for error in self.stats['errors'][:3]:
                print(f"    - {error}")
        
        print("\n" + "="*70)
        print("Test completed successfully! ✓")
        print("="*70 + "\n")
    
    def run_sustained_load_test(self, duration_seconds=60, rate_per_second=100):
        """Run sustained load test"""
        
        print("\n" + "="*70)
        print("Sustained Load Test")
        print("="*70)
        print(f"\nTest Configuration:")
        print(f"  Duration: {duration_seconds} seconds")
        print(f"  Target Rate: {rate_per_second} messages/second")
        print(f"  Expected Total: ~{duration_seconds * rate_per_second:,} messages")
        print("\n" + "="*70)
        
        if not self.connect_kafka():
            return
        
        self.stats['start_time'] = time.time()
        interval = 1.0 / rate_per_second
        end_time = time.time() + duration_seconds
        
        print(f"\n🚀 Starting sustained load at {datetime.now().strftime('%H:%M:%S')}")
        print(f"Sending {rate_per_second} messages/second for {duration_seconds} seconds...\n")
        
        message_count = 0
        last_report = time.time()
        
        while time.time() < end_time:
            self.send_message()
            message_count += 1
            
            # Report progress every 10 seconds
            if time.time() - last_report >= 10:
                elapsed = time.time() - self.stats['start_time']
                current_rate = message_count / elapsed
                print(f"  Progress: {elapsed:.0f}s elapsed | "
                      f"{message_count:,} messages sent | "
                      f"Rate: {current_rate:.1f} msg/s")
                last_report = time.time()
            
            # Sleep to maintain rate
            time.sleep(interval)
        
        self.stats['end_time'] = time.time()
        
        self.producer.flush()
        self.producer.close()
        
        self.print_results()

def print_menu():
    """Print test menu"""
    print("\n" + "="*70)
    print("IoT Streaming Performance Testing Suite")
    print("="*70)
    print("\nAvailable Tests:")
    print("  1. Burst Test (1,000 messages with 4 threads)")
    print("  2. High Volume Test (10,000 messages with 8 threads)")
    print("  3. Stress Test (50,000 messages with 16 threads)")
    print("  4. Sustained Load Test (100 msg/s for 60 seconds)")
    print("  5. Custom Test")
    print("  0. Exit")
    print("="*70)

def run_custom_test():
    """Run custom test with user inputs"""
    try:
        print("\nCustom Test Configuration:")
        total_messages = int(input("  Total messages to send: "))
        num_threads = int(input("  Number of concurrent threads: "))
        
        tester = PerformanceTester()
        tester.run_test(total_messages, num_threads)
    except ValueError:
        print("❌ Invalid input. Please enter numbers only.")
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user.")

def main():
    """Main function"""
    
    while True:
        print_menu()
        
        try:
            choice = input("\nEnter your choice (0-5): ").strip()
            
            if choice == '0':
                print("\nExiting performance tester. Goodbye!")
                break
            elif choice == '1':
                tester = PerformanceTester()
                tester.run_test(total_messages=1000, num_threads=4)
            elif choice == '2':
                tester = PerformanceTester()
                tester.run_test(total_messages=10000, num_threads=8)
            elif choice == '3':
                tester = PerformanceTester()
                tester.run_test(total_messages=50000, num_threads=16)
            elif choice == '4':
                tester = PerformanceTester()
                tester.run_sustained_load_test(duration_seconds=60, rate_per_second=100)
            elif choice == '5':
                run_custom_test()
            else:
                print("❌ Invalid choice. Please enter a number between 0 and 5.")
            
            input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\nExiting performance tester. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
