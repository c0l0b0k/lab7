import asyncio
import aiohttp
from lxml import html
import pika
import os
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "links_queue"

def connect_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    return connection.channel()

async def fetch_links(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            tree = html.fromstring(content)
            base_url = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(url))
            links = tree.xpath("//a[@href]")
            return [
                urljoin(base_url, link.get("href"))
                for link in links
                if "href" in link.attrib
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
