import json
import time
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

BATCH_FILE = "daily_sales.jsonl"


@app.route("/sale", methods=["POST"])
def record_sale():
    data = request.json

    event = {
        "book_id": data["book_id"],
        "title": data["title"],
        "price": float(data["price"]),
        "timestamp": time.time(),
    }

    producer.send("book_sales", event)

    with open(BATCH_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    return jsonify({"status": "success", "event": event}), 201


if __name__ == "__main__":
    app.run(port=5000)
