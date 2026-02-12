import os
import threading
import time
import json
import websocket
import duckdb
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from flask import Flask

# --- 1. THE TRADING BOT CLASS (This was missing) ---
class WallSpectrumLiveMonitor:
    def __init__(self, api_token, start_date, min_c, max_c, min_s, max_s):
        self.api_token = api_token
        self.ws_url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
        self.start_date = start_date
        self.min_cycle = min_c
        self.max_cycle = max_c
        self.min_strength = min_s
        self.max_strength = max_s
        self.running = True
        
        # Use In-Memory DB for Cloud (Faster & No Disk Issues)
        self.con = duckdb.connect(":memory:") 
        self.setup_database()

    def setup_database(self):
        self.con.execute("CREATE TABLE ticks (timestamp BIGINT, price DOUBLE, sort_key BIGINT)")
        self.con.execute("CREATE UNIQUE INDEX idx_sortkey ON ticks (sort_key)")

    def fetch_ticks_chunk(self, task_info):
        start, end = task_info
        try:
            ws = websocket.create_connection(self.ws_url, timeout=10)
            ws.send(json.dumps({"authorize": self.api_token}))
            ws.recv()
            ws.send(json.dumps({
                "ticks_history": "frxEURUSD", "start": start, "end": end, 
                "count": 5000, "style": "ticks", "adjust_start_time": 1
            }))
            res = json.loads(ws.recv())
            ws.close()
            if "history" in res:
                t, p = res["history"]["times"], res["history"]["prices"]
                return [(t[i], p[i], (t[i]*100000)+i) for i in range(len(t))]
            return []
        except: return []

    def backfill_data(self):
        print(f"[SYSTEM] Backfilling from {self.start_date}...")
        now = int(datetime.now(timezone.utc).timestamp())
        req_start = int(self.start_date.timestamp())
        
        tasks = []
        curr = req_start
        while curr < now:
            end = min(now, curr + 600)
            tasks.append((curr, end))
            curr = end

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.fetch_ticks_chunk, t): t for t in tasks}
            for future in as_completed(futures):
                data = future.result()
                if data:
                    self.con.executemany("INSERT OR IGNORE INTO ticks VALUES (?, ?, ?)", data)
        print("[SYSTEM] Backfill Complete.")

    def tick_collector(self):
        while self.running:
            try:
                ws = websocket.create_connection(self.ws_url)
                ws.send(json.dumps({"authorize": self.api_token}))
                ws.recv()
                ws.send(json.dumps({"ticks": "frxEURUSD", "subscribe": 1}))
                while self.running:
                    data = json.loads(ws.recv())
                    if "tick" in data:
                        t = data["tick"]["epoch"]
                        p = data["tick"]["quote"]
                        self.con.execute("INSERT OR IGNORE INTO ticks VALUES (?, ?, ?)", (t, p, t*100000))
            except:
                time.sleep(5)

    def analyze(self):
        while self.running:
            time.sleep(60) # Analyze every minute
            try:
                # Basic Analysis Logic Placeholder
                count = self.con.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
                last = self.con.execute("SELECT price FROM ticks ORDER BY sort_key DESC LIMIT 1").fetchone()
                price = last[0] if last else 0
                print(f"[LIVE CHECK] Ticks: {count} | Price: {price} | Status: OK")
            except Exception as e:
                print(f"[ERROR] {e}")

    def run(self):
        self.backfill_data()
        threading.Thread(target=self.tick_collector, daemon=True).start()
        self.analyze()

# --- 2. THE CLOUD SERVER SETUP ---
app = Flask(__name__)

@app.route('/')
def keep_alive():
    return "WallSpectrum Monitor is Running! 🟢"

def run_bot():
    # Retrieve Token or use Default
    API_TOKEN = os.environ.get("DERIV_TOKEN", "TpVIBWpqet5X8AH")
    
    # Start Date: 2 Days ago
    start_date = datetime.now(timezone.utc) - timedelta(days=2)
    
    print("Starting Background Worker...")
    bot = WallSpectrumLiveMonitor(
        api_token=API_TOKEN, 
        start_date=start_date, 
        min_c=60, max_c=300, 
        min_s=70, max_s=100
    )
    bot.run()

# --- 3. ENTRY POINT ---
if __name__ == "__main__":
    # Start Bot in Background
    t = threading.Thread(target=run_bot)
    t.daemon = True
    t.start()
    
    # Start Fake Web Server (Port provided by Render)
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
