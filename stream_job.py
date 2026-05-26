import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "book_sales",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

window_data = []
WINDOW_SIZE_SEC = 300

print("Запуск потоковой обработки. Ожидание данных...")

for message in consumer:
    event = message.value
    current_time = time.time()

    window_data.append((event["timestamp"], event["price"]))

    window_data = [
        item for item in window_data if current_time - item[0] <= WINDOW_SIZE_SEC
    ]

    if window_data:
        total_sum = sum(item[1] for item in window_data)
        avg_price = total_sum / len(window_data)

        print(f"[STREAM] Событие: продана '{event['title']}' за {event['price']}$.")
        print(
            f"         Скользящее среднее (5 мин): {avg_price:.2f}$ (Транзакций в окне: {len(window_data)})"
        )
        print("-" * 50)
