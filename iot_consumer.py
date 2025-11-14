# iot_consumer.py
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
import time
from collections import defaultdict

class IoTConsumer:
    def __init__(self, topics, group_id='iot-analytics'):
        """Initialize consumer with configuration"""
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=['localhost:9092'],
            group_id=group_id,
            value_deserializer=self._safe_deserialize,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Manual commit for exactly-once
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        self.metrics = defaultdict(list)
        self.alert_thresholds = {
            'temperature': (15, 30),
            'humidity': (30, 70),
            'pressure': (1000, 1030)
        }
    
    def _safe_deserialize(self, message_bytes):
    	"""Safe JSON deserializer with error handling"""
    	if message_bytes is None:
             return None
    	try:
             return json.loads(message_bytes.decode('utf-8'))
    	except (json.JSONDecodeError, UnicodeDecodeError) as e:
             print(f"⚠️ Deserialization error: {e}")
             return None

    def process_message(self, message):
        """Process individual sensor reading"""
        data = message.value
        sensor_type = data['sensor_type']
        value = data['value']
        
        # Store metrics
        self.metrics[sensor_type].append(value)
        
        # Check for alerts
        if sensor_type in self.alert_thresholds:
            min_val, max_val = self.alert_thresholds[sensor_type]
            if value < min_val or value > max_val:
                self.trigger_alert(data, min_val, max_val)
        
        # Calculate statistics every 10 readings
        if len(self.metrics[sensor_type]) >= 10:
            self.calculate_statistics(sensor_type)
            self.metrics[sensor_type] = self.metrics[sensor_type][-10:]
        
        return True
    
    def trigger_alert(self, data, min_val, max_val):
        """Handle threshold violations"""
        print(f"🚨 ALERT: {data['sensor_id']} - {data['sensor_type']} = {data['value']} "
              f"(threshold: {min_val}-{max_val})")
        # Here you could send notifications, write to database, etc.
    
    def calculate_statistics(self, sensor_type):
        """Calculate running statistics"""
        values = self.metrics[sensor_type]
        if values:
            avg = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            print(f"📊 Stats for {sensor_type}: "
                  f"Avg={avg:.2f}, Min={min_val:.2f}, Max={max_val:.2f}")
    
    def consume_messages(self):
        """Main consumption loop with error handling"""
        print("🎧 Starting IoT consumer...")

        try:
            for message in self.consumer:
                try:
                    success = self.process_message(message)
                    if success:
                        self.consumer.commit()
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    # Could write to dead letter queue here
                    continue

                # Print consumer lag periodically
                self.print_consumer_lag()

        except KeyboardInterrupt:
            print("\n⏹️ Stopping consumer...")
        finally:
            self.consumer.close()
            print("✅ Consumer stopped")

    
    def print_consumer_lag(self):
        """Monitor consumer lag"""
        for partition in self.consumer.assignment():
            committed = self.consumer.committed(partition)
            position = self.consumer.position(partition)
            if committed:
                lag = position - committed
                if lag > 100:
                    print(f"⚠️ High lag on {partition}: {lag} messages")

# Run the consumer
if __name__ == "__main__":
    consumer = IoTConsumer(['iot-sensors'])
    consumer.consume_messages()
