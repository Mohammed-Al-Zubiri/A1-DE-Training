import json
import time
import random
import os
from datetime import datetime
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration
OUTPUT_DIR = os.path.expanduser("~/nifi_input_data")
BATCH_INTERVAL_SECONDS = 5
RECORDS_PER_BATCH = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_messy_record():
    """Generates a single semi-structured e-commerce record with intentional anomalies."""
    record_id = fake.uuid4()
    
    # Base Record
    record = {
        "transaction_id": record_id,
        "user_id": fake.random_int(min=1000, max=99999),
        "product_name": fake.catch_phrase(),
        "price": round(random.uniform(5.0, 1200.0), 2),
        "payment_method": random.choice(["Credit Card", "PayPal", "Crypto", "Bank Transfer"]),
        "timestamp": datetime.now().isoformat()
    }

    # Inject Messiness (Missing values, inconsistent formats)
    mess_factor = random.random()
    
    if mess_factor < 0.10:
        # 10% chance: Missing price (Null value)
        record["price"] = None
    elif mess_factor < 0.20:
        # 10% chance: Inconsistent date format (Unix epoch int instead of ISO string)
        record["timestamp"] = int(time.time())
    elif mess_factor < 0.30:
        # 10% chance: Missing product name entirely
        record.pop("product_name", None)
        
    return record

def main():
    print(f"Starting real-time e-commerce data simulation...")
    print(f"Writing batches to '{OUTPUT_DIR}/' every {BATCH_INTERVAL_SECONDS} seconds.")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            batch_data = []
            
            # Generate a batch of records
            for _ in range(RECORDS_PER_BATCH):
                record = generate_messy_record()
                batch_data.append(record)
                
                # Inject Duplicates
                if random.random() < 0.05: 
                    # 5% chance to append the exact same record twice
                    batch_data.append(record)

            # Define the file name with a timestamp
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"transactions_batch_{timestamp_str}.json"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Write as newline-delimited JSON (NDJSON)
            # Highly optimized for NiFi's record readers
            with open(file_path, "w") as f:
                for item in batch_data:
                    f.write(json.dumps(item) + "\n")
                    
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Wrote {len(batch_data)} records to {file_name}")
            
            # Pause before the next batch
            time.sleep(BATCH_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\nSimulation stopped.")

if __name__ == "__main__":
    main()
