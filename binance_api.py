import asyncio
import websockets
import json
import time
from dotenv import load_dotenv
import os

load_dotenv()

num_connect = int(os.getenv('NUM_CONNECTIONS'))
period = int(os.getenv('COLLECTION_PERIOD'))
url = os.getenv('BINANCE_WS_URL')
stream_name = os.getenv('STREAM_NAME').split("@")[0].lower() + "@" + os.getenv('STREAM_NAME').split("@")[1]
output_file = os.getenv('OUTPUT_FILE')


async def subscribe(ws, id):
    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": [stream_name], "id": id}))
    response = await ws.recv()
    response_data = json.loads(response)
    if "result" in response_data and response_data["result"] is None:
        print(f"Подключение {id}: Подписка на {stream_name} успешна.")
        return True
    else:
        print(f"Подключение {id}: Подписка не удалась. Ответ сервера: {response}")
        return False
     
async def collect_data(connection_id):
    try:
        async with websockets.connect(url) as websocket:
            print(f"Подключение {connection_id}: Подключено к WebSocket.")
            if await subscribe(websocket, connection_id):
                start_time = time.time()
                data = []
                while time.time() - start_time < period:
                    message = await websocket.recv()
                    message_data = json.loads(message)
                    message_data["receive_time"] = time.time() * 1000
                    data.append(message_data)
                return {"connection_id": connection_id, "data": data}
            else:
                return []
    except Exception as e:
        print(f"Подключение {connection_id}: Произошла ошибка: {e}")
        return []

async def main():
    tasks = [collect_data(i) for i in range(num_connect)]
    if tasks:
        all_data = await asyncio.gather(*tasks)
        with open(output_file, "w") as f:
            json.dump(all_data, f, indent=4)
        print(f"Собранные данные записаны в JSON файл ./{output_file}")
    else:
        print(f"Не удалось получить данные из раздачи маркет даты")


if __name__ == "__main__":
    print(f'Сбор данных....')
    asyncio.run(main())
