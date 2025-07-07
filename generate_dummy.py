import random 
import time 
from datetime import datetime, timedelta
import json 
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'transactions-stream'

LOCATIONS = ['jakarta', 'bandung', 'cimahi', 'depok', 'bogor', 'surabaya', 'malang', 'semarang', 'yogyakarta']
MERCHANTS = ['alfamart', 'indomaret', 'tokopedia', 'shopee', 'bukalapak']
PAYMENT_METHODS = ['Debit Card', 'Credit Card', 'E-Wallet', 'Q-ris']
BLACKLISTED_CUSTOMERS = ['C000999', 'C000888', 'cust-123', 'cust-234']
BLACKLISTED_DEVICES = ['dev-black-001', 'dev-black-002', 'dev-234', 'dev-123', 'device_314', 'device_423']

geo_customers = {}


fraud_types = ['high_value', 'blacklist', 'geo_anomaly', 'device_inconsistency', 'velocity']
normal_counter = 0 
fraud_interval_range = (50, 70)
next_fraud_at = random.randint(*fraud_interval_range)

# Kafka producer 
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer = lambda k: str(k).encode('utf-8'),
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)


# Function 
def generate_id_from_timestamp(ts: datetime):
    return "TX" + ts.strftime("%Y%m%d%H%M%S")

def generate_transaction( customer_id=None, device_id=None, location=None, amount=None,):
    timestamp = datetime.now()
    transaction_id = generate_id_from_timestamp(timestamp)
    customer_id = customer_id or "C" + str(random.randint(100000, 999999))
    device_id = device_id or "dev-" + str(random.randint(1000, 9999))
    location = location or random.choice(LOCATIONS)
    amount = amount or random.randint(10_000, 5_000_000)

    return {
        "transaction_id": transaction_id,
        "customer_id": customer_id,
        "amount": amount,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "merchant": random.choice(MERCHANTS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "location": location,
        "device_id": device_id
    }

# ======================== STREAMING ========================
print("📡 Streaming dummy data ke Kafka... (CTRL+C to stop)")
try:
    while True:
        is_fraud = normal_counter>=next_fraud_at
        fraud_type = random.choice(fraud_types) if is_fraud else None
        tx = None 

        if fraud_type == "velocity":
            velocity_customer = "C" + str(random.randint(100000, 999999))
            velocity_device = "dev-" + str(random.randint(1000, 9999))
            velocity_location = random.choice(LOCATIONS)
            
            for _ in range(5):
                tx = generate_transaction(
                    customer_id = velocity_customer,
                    device_id  = velocity_device,
                    location = velocity_location
                )

                producer.send(KAFKA_TOPIC, key= tx["transaction_id"], value=tx)
                print("🚀 [velocity] sent:", json.dumps(tx))
                time.sleep(1)

            normal_counter = 0 
            next_fraud_at = random.randint(*fraud_interval_range)
            continue
        
        elif fraud_type == "high_value":
            tx = generate_transaction(amount=random.randint(10_000_001, 20_000_000))
            normal_counter = 0 
            next_fraud_at = random.randint(*fraud_interval_range)

        elif fraud_type == "blacklist":
            if random.random() < 0.5:
                cid = random.choice(BLACKLISTED_CUSTOMERS)
                tx = generate_transaction(customer_id=cid)
            else:
                did = random.choice(BLACKLISTED_DEVICES)
                tx = generate_transaction(device_id=did)


            normal_counter = 0 
            next_fraud_at = random.randint(*fraud_interval_range)

        elif fraud_type == "geo_anomaly":
            if geo_customers:
                cid = random.choice(list(geo_customers.keys()))
                prev_loc, prev_time = geo_customers[cid]
                new_loc = random.choice([l for l in LOCATIONS if l != prev_loc])
                tx = generate_transaction(customer_id=cid, location=new_loc)
                geo_customers[cid] = (new_loc, tx["timestamp"])
            else:
                # populasi awal customer lokasi
                for _ in range(5):
                    tx_init = generate_transaction()
                    geo_customers[tx_init["customer_id"]] = (tx_init["location"], datetime.now())
                continue  # skip kali ini dulu, karena belum bisa fraud

            normal_counter = 0 
            next_fraud_at = random.randint(*fraud_interval_range)

        elif fraud_type == "device_inconsistency":
            shared_device = "dev-" + str(random.randint(1000, 9999))

            for _ in range(2):
                tx = generate_transaction(device_id=shared_device)

                producer.send(KAFKA_TOPIC, key=tx["transaction_id"], value=tx)
                print("🚨 [device_inconsistency] sent:", json.dumps(tx))
                time.sleep(1)

            normal_counter = 0
            next_fraud_at = random.randint(*fraud_interval_range)
            continue
        else:
            tx = generate_transaction()
            normal_counter += 1
        
        producer.send(KAFKA_TOPIC, key=tx["transaction_id"], value=tx)
        print("\u2705 sent:", json.dumps(tx))
        time.sleep(1)

except KeyboardInterrupt:
    print("\n⛔️ Streaming stopped.")
    producer.flush()
    producer.close()




