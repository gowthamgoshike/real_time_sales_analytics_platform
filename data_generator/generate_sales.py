import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = list(range(1,50))
stores = list(range(1,10))

def generate_sale():

    sale = {
        "transaction_id": random.randint(100000,999999),
        "product_id": random.choice(products),
        "store_id": random.choice(stores),
        "price": round(random.uniform(10,500),2),
        "quantity": random.randint(1,5),
        "timestamp": datetime.now().isoformat()
    }

    return sale


while True:

    data = generate_sale()

    producer.send("sales_topic", data)

    print("Sent:", data)

    time.sleep(0.5)