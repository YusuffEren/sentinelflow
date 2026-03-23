import time
import requests
import random
import sys
from datetime import datetime
from sentinelflow.generator.patterns import FraudPatternMixer
from sentinelflow.generator.models import Transaction, FraudType

API_URL = "http://localhost:8000/api/v1/transactions"


def send_transaction(tx: Transaction):
    """Send transaction to API via HTTP POST."""
    data = tx.to_dict()
    # Ensure timestamp is string for JSON
    data["timestamp"] = data["timestamp"].isoformat()
    # Convert IDs to string if they are UUIDs
    data["transaction_id"] = str(data["transaction_id"])
    data["sender_account_id"] = str(data["sender_account_id"])
    data["receiver_account_id"] = str(data["receiver_account_id"])

    try:
        response = requests.post(API_URL, json=data)
        if response.status_code == 200:
            result = response.json()
            status = "FRAUD DETECTED" if result["is_fraud"] else "OK"
            color = "\033[91m" if result["is_fraud"] else "\033[92m"  # Red or Green
            reset = "\033[0m"
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] {color}{status}{reset} | {tx.amount:.2f} {tx.currency} | {tx.sender_name} -> {tx.receiver_name}"
            )
        else:
            print(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Failed to connect to API: {e}")


def main():
    print("Starting HTTP Transaction Generator...")
    print(f"Target: {API_URL}")
    print("----------------------------------------")

    mixer = FraudPatternMixer(fraud_ratio=0.1)  # 10% fraud chance

    try:
        while True:
            # Generate a batch of 1-3 transactions
            batch_size = random.randint(1, 3)
            batch = mixer.generate_batch(batch_size)

            for tx in batch:
                send_transaction(tx)
                time.sleep(random.uniform(0.1, 0.5))  # Slight delay between txs in batch

            # scalable delay
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nGenerator stopped.")


if __name__ == "__main__":
    main()
