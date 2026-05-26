import pandas as pd
import json

BATCH_FILE = "daily_sales.jsonl"


def run_batch_job():
    print("Запуск ежедневного пакетного джоба...")

    try:
        data = []
        with open(BATCH_FILE, "r", encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line.strip()))

        if not data:
            print("Нет данных для обработки.")
            return

        df = pd.DataFrame(data)

        total_revenue = df["price"].sum()

        top_books = df.groupby("title").size().reset_index(name="sales_count")
        top_books = top_books.sort_values(by="sales_count", ascending=False).head(5)

        print("\n=== ЕЖЕДНЕВНЫЙ ОТЧЕТ ===")
        print(f"Общая выручка за день: {total_revenue:.2f}$")
        print("\nТоп-5 продаваемых книг:")
        for index, row in top_books.iterrows():
            print(f"- {row['title']}: {row['sales_count']} шт.")
        print("========================")

    except FileNotFoundError:
        print(f"Файл {BATCH_FILE} не найден.")


if __name__ == "__main__":
    run_batch_job()
