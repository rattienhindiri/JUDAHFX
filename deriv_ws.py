import websocket
import json
import pandas as pd

def on_message(ws, message):
    data = json.loads(message)

    # Check if the expected key exists
    if 'candles' in data:
        candles = data['candles']
        df = pd.DataFrame(candles)

        # Ensure 'epoch' exists before converting
        if 'epoch' in df.columns:
            df['epoch'] = pd.to_datetime(df['epoch'], unit='s')
            df.to_csv('volatility_100.csv', index=False)
            print("Saved to volatility_100.csv")
        else:
            print("Missing 'epoch' in data")

        ws.close()
    else:
        print("Unexpected data received:", data)

def on_open(ws):
    request = {
        "ticks_history": "R_100",
        "adjust_start_time": 1,
        "count": 500,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": 60  # 1 minute candles
    }
    ws.send(json.dumps(request))

# Corrected WebSocket endpoint with a public app_id
ws = websocket.WebSocketApp("wss://ws.binaryws.com/websockets/v3?app_id=1089",
                            on_open=on_open,
                            on_message=on_message)

ws.run_forever()
