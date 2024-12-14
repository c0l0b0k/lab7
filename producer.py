import asyncio
import aiohttp
from lxml import html
import pika
import os
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from urllib.parse import quote, urlparse, urlunparse

# Загрузка переменных окружения
load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "links_queue"

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


async def main(url):
    channel = connect_rabbitmq()
    channel.queue_declare(queue=QUEUE_NAME)
    links = await fetch_links(url)
    for link in links:
        print(f"Found link: {link}")
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=link)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python producer.py <URL>")
        exit(1)
    url = sys.argv[1]
    asyncio.run(main(url))
