import asyncio
import aiohttp
from lxml import html
import pika
import os
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from time import time, sleep
from urllib.parse import quote, urlparse, urlunparse

# Загрузка переменных окружения
load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "links_queue"
TIMEOUT = 10  # Тайм-аут в секундах

def connect_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    return connection.channel()

def fix_url(url):
    parsed = urlparse(url)
    # Кодируем только путь и параметры (оставляя схему и хост без изменений)
    fixed_path = quote(parsed.path)
    return urlunparse((parsed.scheme, parsed.netloc, fixed_path, parsed.params, parsed.query, parsed.fragment))

async def fetch_links(url):
    url = fix_url(url)  # Исправляем URL перед запросом
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content_type = response.headers.get('Content-Type', '')
            if not content_type.startswith('text/html'):
                print(f"Skipping non-HTML content: {url} (Content-Type: {content_type})")
                return []

            try:
                content = await response.text()
            except UnicodeDecodeError:
                print(f"Failed to decode content from: {url}")
                return []

            tree = html.fromstring(content)
            base_url = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(url))
            links = tree.xpath("//a[@href]")
            return [
                urljoin(base_url, link.get("href"))
                for link in links
                if "href" in link.attrib and urlparse(urljoin(base_url, link.get("href"))).scheme in ("http", "https")
            ]


def process_message(channel, method, properties, body):
    url = body.decode("utf-8")
    print(f"Processing: {url}")
    asyncio.run(process_url(url))

async def process_url(url):
    links = await fetch_links(url)
    channel = connect_rabbitmq()
    for link in links:
        print(f"Adding link to queue: {link}")
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=link)

def main():
    channel = connect_rabbitmq()
    channel.queue_declare(queue=QUEUE_NAME)
    print("Waiting for messages. To exit, press CTRL+C")

    last_message_time = time()  # Время последнего сообщения

    while True:
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)

        if method_frame:
            last_message_time = time()  # Обновляем время последнего сообщения
            process_message(channel, method_frame, header_frame, body)
        else:
            # Проверяем, истёк ли тайм-аут
            if time() - last_message_time > TIMEOUT:
                print("Queue is empty for too long. Exiting.")
                break
            sleep(1)  # Ждём перед следующей проверкой

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Consumer stopped.")
