import json
import random
import time
from datetime import datetime, timezone

devices = [f"sensor_{i}" for i in range(1, 11)]

def generate_record():
    return {
        "device_id": random.choice(devices),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(20, 100), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "pressure": round(random.uniform(90, 110), 2),
        "status": random.choice(["active", "inactive"])
    }

while True:
    with open("../sample_data/iot_data.json", "a") as f:
        json.dump(generate_record(), f)
        f.write("\n")
    
    print("Generated one record...", flush=True)
    time.sleep(1)