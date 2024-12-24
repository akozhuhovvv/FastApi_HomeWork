import os
import json
import threading
import time
import traceback
from typing import Any, Dict

import pika
from fastapi import FastAPI
from pydantic import BaseModel
from clickhouse_driver import Client
import uvicorn

###############################################################################
#                     1. Конфигурация из переменных окружения                 #
###############################################################################
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_DEFAULT_USER", "user")
RABBITMQ_PASS = os.environ.get("RABBITMQ_DEFAULT_PASS", "password")
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = "data_queue"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = ""
CLICKHOUSE_DB = "default"

###############################################################################
#                  2. Подключение к ClickHouse и создание таблицы             #
###############################################################################
ch_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASS,
    database=CLICKHOUSE_DB
)

ch_client.execute('''
    CREATE TABLE IF NOT EXISTS test_table (
        id String,
        name String
    )
    ENGINE = Log
''')


###############################################################################
#                           3. Функции RabbitMQ                               #
###############################################################################
def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
    )


def publish_message_to_queue(data: Dict[str, Any]):
    """Отправляет сообщение (в формате JSON) в очередь RabbitMQ."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    message = json.dumps(data)
    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_QUEUE,
        body=message
    )
    connection.close()


def consume_messages():
    """
    Постоянно читает сообщения из очереди RabbitMQ и сохраняет их в ClickHouse.
    Запускается в отдельном потоке при старте приложения.

    Использует цикл с ретраями, чтобы если брокер не доступен (или упадёт),
    через несколько секунд попробовать заново.
    """
    while True:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE)

            def callback(ch, method, properties, body):
                try:
                    data = json.loads(body)
                    # Предположим, что в data есть поля "id" и "name"
                    ch_client.execute(
                        "INSERT INTO test_table (id, name) VALUES",
                        [(data.get('id', ''), data.get('name', ''))]
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print("Ошибка при обработке сообщения:", e)
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
            print("[*] Запущен потребитель RabbitMQ. Ожидание сообщений...")
            channel.start_consuming()
        except Exception as e:
            print("Ошибка в consume_messages (RabbitMQ недоступен?):", e)
            traceback.print_exc()
            # Подождём 5 секунд и попробуем снова
            time.sleep(5)


###############################################################################
#                           4. FastAPI-приложение                              #
###############################################################################
app = FastAPI()


class DataModel(BaseModel):
    id: str
    name: str


@app.post("/send")
def send_data(data: DataModel):
    """
    Принимает JSON-данные и отправляет их в очередь RabbitMQ.
    Пример тела запроса:
    {
        "id": "123",
        "name": "My Name"
    }
    """
    publish_message_to_queue(data.dict())
    return {"status": "Message sent"}


@app.get("/data")
def get_data():
    """
    Возвращает все данные из ClickHouse (таблица test_table).
    """
    rows = ch_client.execute("SELECT id, name FROM test_table")
    result = [{"id": row[0], "name": row[1]} for row in rows]
    return {"data": result}


###############################################################################
#                5. Запуск обработчика RabbitMQ в отдельном потоке            #
###############################################################################
@app.on_event("startup")
def startup_event():
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    print("[*] Поток-потребитель запущен")


###############################################################################
#                   6. Точка входа при обычном запуске Python                 #
###############################################################################
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
