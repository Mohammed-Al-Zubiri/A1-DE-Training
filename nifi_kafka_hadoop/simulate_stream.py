import csv
import time
import random
import uuid
import os
from datetime import datetime

# E-commerce orders scenario
OUTPUT_DIR = "./streaming_data_drop"

def generate_timestamp():
    """Generates different timestamp formats to simulate inconsistency."""
    now = datetime.now()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y%m%d%H%M%S",
        "%Y-%m-%dT%H:%M:%S.%fZ" 
    ]
    return now.strftime(random.choice(formats))

def generate_record():
    """Generates a single messy e-commerce order record."""
    transaction_id = str(uuid.uuid4())[:8]
    customer_id = f"CUST{random.randint(100, 999)}"
    
    amount = round(random.uniform(-50.0, 1500.0), 2) if random.random() > 0.1 else random.choice([-999.99, 999999.99])
    timestamp = generate_timestamp()
    
    if random.random() < 0.1:
        customer_id = ""
        
    record = [transaction_id, customer_id, amount, timestamp]
    
    if random.random() < 0.05:
        record.append("CORRUPTED_DATA_STRING")
    elif random.random() < 0.05:
        record = [transaction_id, "INVALID", "NaN"]
        
    return record

def stream_files():
    """Continuously creates new CSV files to simulate a multi-file stream."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    print(f"Starting multi-file stream to {OUTPUT_DIR}...")
    
    batch_number = 1
    previous_record = None

    while True:
        # Create a unique filename based on the current timestamp
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ecommerce_batch_{current_time}_{batch_number}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Generate enough records to ensure files exceed 100 KB to test NiFi chunking
        records_in_batch = random.randint(1500, 4000)
        
        with open(filepath, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["transaction_id", "customer_id", "amount", "timestamp"])
            
            for _ in range(records_in_batch):
                if previous_record and random.random() < 0.08:
                    record = previous_record
                else:
                    record = generate_record()
                    
                writer.writerow(record)
                previous_record = record
                
        print(f"Generated {filename} with {records_in_batch} records.")
        batch_number += 1
        
        # Wait a few seconds before dropping the next file
        time.sleep(random.uniform(2.0, 5.0))

if __name__ == "__main__":
    try:
        stream_files()
    except KeyboardInterrupt:
        print("\nStreaming generator stopped.")
