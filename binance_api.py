import asyncio
import websockets
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv()

num_connect = int(os.getenv('NUM_CONNECTIONS'))
period = int(os.getenv('COLLECTION_PERIOD'))
url = os.getenv('BINANCE_WS_URL')
stream_name = os.getenv('STREAM_NAME')
output_file = os.getenv('OUTPUT_FILE')


async def subscribe(ws):
    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": [stream_name], "id": 1}))
     
async def collect_data(connection_id):
    async with websockets.connect(url) as websocket:
        await subscribe(websocket)
        start_time = time.time()
        data = []
        while time.time() - start_time < period:
            message = await websocket.recv()
            message_json = json.loads(message)
            message_json["connection_id"] = connection_id
            message_json["timestamp"] = datetime.now(timezone.utc).isoformat()
            data.append(message_json)
        return data

async def main():
    tasks = [collect_data(i) for i in range(num_connect)]
    all_data = await asyncio.gather(*tasks)

    print(f"Собранные данные записаны в JSON файл ./{output_file}")
    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=4)


if __name__ == "__main__":
    print(f'Сбор данных....')
    asyncio.run(main())
