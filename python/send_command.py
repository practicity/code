from kafka import KafkaProducer
import json
import time

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'jaamsim-commands'

def toggle_button(entity_name):
    """Flip the button - if ON becomes OFF, if OFF becomes ON"""
    msg = {"entity": entity_name, "action": "TOGGLE"}
    producer.send(TOPIC, msg)
    producer.flush()
    print(f"Sent TOGGLE to {entity_name}")

def set_button(entity_name, value):
    """Set button to a specific state - True=pressed, False=unpressed"""
    msg = {"entity": entity_name, "action": "SET", "value": value}
    producer.send(TOPIC, msg)
    producer.flush()
    print(f"Sent SET {value} to {entity_name}")

# =====================================================
# EXAMPLES - uncomment what you want to do
# =====================================================

# Example 1: Toggle ToggleButton1 (flip its state)
toggle_button("ToggleButton1")

# Example 2: Stop the generator (set button to FALSE)
# set_button("ToggleButton1", False)

# Example 3: Start the generator (set button to TRUE)
# set_button("ToggleButton1", True)

# Example 4: Control ToggleButton2
# set_button("ToggleButton2", False)

# Example 5: Automated sequence - stop for 10 seconds then restart
# print("Stopping machine...")
# set_button("ToggleButton1", False)
# time.sleep(10)
# print("Restarting machine...")
# set_button("ToggleButton1", True)

producer.close()
