from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'jaamsim-commands'

def send(msg):
    producer.send(TOPIC, msg)
    producer.flush()
    print(f"  -> Sent: {msg}")

print("=" * 60)
print("JaamSim Remote Control")
print("=" * 60)
print()
print("--- Toggle Buttons ---")
print("  1 = Toggle ToggleButton1")
print("  2 = Toggle ToggleButton2")
print()
print("--- Simulation Control ---")
print("  s = Start/Resume simulation")
print("  p = Pause simulation")
print("  x = Stop simulation")
print()
print("--- Entity Control ---")
print("  e  = Edit any entity input")
print("  qe = Query any entity input")
print()
print("  q = Quit")
print()

while True:
    choice = input("Enter command: ").strip().lower()

    if choice == '1':
        send({"entity": "ToggleButton1", "action": "TOGGLE"})
    elif choice == '2':
        send({"entity": "ToggleButton2", "action": "TOGGLE"})
    elif choice == 's':
        send({"action": "SIM_START"})
    elif choice == 'p':
        send({"action": "SIM_PAUSE"})
    elif choice == 'x':
        send({"action": "SIM_STOP"})
    elif choice == 'e':
        entity = input("  Entity name: ").strip()
        inp = input("  Input name: ").strip()
        val = input("  New value: ").strip()
        send({"entity": entity, "action": "EDIT", "input": inp, "value": val})
    elif choice == 'qe':
        entity = input("  Entity name: ").strip()
        inp = input("  Input name: ").strip()
        send({"entity": entity, "action": "QUERY", "input": inp})
    elif choice == 'q':
        break
    else:
        print("  Unknown command")

producer.close()
print("Bye!")
