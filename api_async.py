import asyncio
import json
import os
import time
import websockets

from dotenv import load_dotenv
from typing import (
    Final,
    Union,
    Dict,
    Any,
)


load_dotenv()

NUM_CONNECTIONS: Final = int(os.getenv('NUM_CONNECTIONS'))
COLLECTION_PERIOD: Final = int(os.getenv('COLLECTION_PERIOD'))
BINANCE_WS_URL: Final = os.getenv('BINANCE_WS_URL')
STREAM_NAME: Final = os.getenv('STREAM_NAME').split("@")[0].lower() + "@" + os.getenv('STREAM_NAME').split("@")[1]
OUTPUT_FILE: Final = os.getenv('OUTPUT_FILE_1')


async def subscribe(ws: websockets.WebSocketClientProtocol, stream_name: str, connection_id: int) -> bool:
    """Subscribe to the WebSocket stream."""
    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": [stream_name], "id": connection_id}))
    response = await ws.recv()
    response_data = json.loads(response)

    if response_data.get("result") is None:
        print(f"Connection {connection_id}: Successfully subscribed to {stream_name}.")
        return True
    else:
        print(f"Connection {connection_id}: Subscription failed. Server response: {response}")
        return False


async def collect_data(connection_id: int) -> Union[None, Dict[str, Any]]:
    """Collect data from the WebSocket stream."""
    try:
        async with websockets.connect(BINANCE_WS_URL) as websocket:
            print(f"Connection {connection_id}: Connected to WebSocket.")
            
            if not await subscribe(websocket, STREAM_NAME, connection_id):
                return []

            start_time = time.time()
            data = []

            while time.time() - start_time <= COLLECTION_PERIOD:
                message = await websocket.recv()
                message_data = json.loads(message)
                message_data["receive_time"] = int(time.time() * 1000)
                data.append(message_data)

            return {"connection_id": connection_id, "data": data}

    except Exception as e:
        print(f"Connection {connection_id}: Error occurred: {e}")
        return []


async def main() -> None:
    """Main function to start data collection."""
    tasks = [collect_data(i) for i in range(NUM_CONNECTIONS)]

    if tasks:
        all_data = await asyncio.gather(*tasks)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(all_data, f, indent=4)
        print(f"Collected data saved to ./{OUTPUT_FILE}")
    else:
        print("No tasks were executed.")


if __name__ == "__main__":
    print('Starting data collection...')
    asyncio.run(main())
