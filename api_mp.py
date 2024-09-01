import json
from multiprocessing import Process, Manager
import time
import os
from websockets.sync.client import connect
from dotenv import load_dotenv
from typing import Final

load_dotenv()

NUM_CONNECTIONS: Final = int(os.getenv('NUM_CONNECTIONS'))
COLLECTION_PERIOD: Final = int(os.getenv('COLLECTION_PERIOD'))
BINANCE_WS_URL: Final = os.getenv('BINANCE_WS_URL')
STREAM_NAME: Final = os.getenv('STREAM_NAME').split("@")[0].lower() + "@" + os.getenv('STREAM_NAME').split("@")[1]
OUTPUT_FILE: Final = os.getenv('OUTPUT_FILE_3')


def collect_data(connection_id: int) -> None:
    """Collect data from the WebSocket stream."""
    data = []
    try:
      with connect(f"{BINANCE_WS_URL}/{STREAM_NAME}") as websocket:
          print(f"Connection {connection_id}: Connected to WebSocket.")
          
          start_time = time.time()
          data = []
          while time.time() - start_time <= COLLECTION_PERIOD:
              message = websocket.recv()
              message_data = json.loads(message)
              message_data["receive_time"] = int(time.time() * 1000)
              data.append(message_data)   
    except Exception as e:
        print(f"Connection {connection_id}: Error occurred: {e}")  
        
    return {"connection_id": connection_id, "data": data}

def main() -> None:
    """Main function to start data collection."""
    manager = Manager()
    data_list = manager.list()  # Shared list for storing data from all processes

    processes = []
    for i in range(NUM_CONNECTIONS):
        p = Process(target=lambda q, id: q.append(collect_data(id)), args=(data_list, i))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()

    # Organize data into the required JSON structure
    all_data = list(data_list)
    
    # Save data to JSON file
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_data, f, indent=4)
    
    print(f"Collected data saved to ./{OUTPUT_FILE}")


if __name__ == "__main__":
    print('Starting data collection...')
    main()