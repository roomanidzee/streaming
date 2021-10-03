import asyncio
from pathlib import Path
from typing import List
import json

from aiokafka import AIOKafkaProducer

from domain import PersonRecord, read_data_from_csv


# limit just for testing purposes
async def send(loop, records: List[PersonRecord], limit=1000000):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers="localhost:9093",)
    await producer.start()

    counter = 0

    for record in records:

        await producer.send_and_wait("persons-json", json.dumps(record.to_json()).encode())
        await producer.send_and_wait("persons-avro", record.serialize())
        counter += 1

        print(f'sent record {record}')

        if counter == limit:
            break

    else:
        await producer.stop()
        print("Stoping producer...")

    await producer.stop()

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parents[2] / Path("resources") / Path("data")

    persons = read_data_from_csv(data_path)

    limit_value = 100000

    async_loop = asyncio.get_event_loop()
    tasks = asyncio.gather(send(async_loop, persons, limit_value))

    async_loop.run_until_complete(tasks)
