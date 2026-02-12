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
from flask import Flask, jsonify, render_template_string

# --- CONFIGURATION (Change these if you want) ---
CONFIG = {
    "SYMBOL": "frxEURUSD",
    "MIN_CYCLE": 60,
    "MAX_CYCLE": 300,
    "MIN_STRENGTH": 70,
    "MAX_STRENGTH": 100,
    "LOOKBACK_DAYS": 2
}

# --- GLOBAL STORAGE ---
LATEST_DATA = {
    "status": "Starting...",
    "last_update": "Waiting...",
    "price": 0,
    "kings": [],
    "settings": f"{CONFIG['MIN_CYCLE']}-{CONFIG['MAX_CYCLE']}s | {CONFIG['MIN_STRENGTH']}-{CONFIG['MAX_STRENGTH']}%"
}

# --- 1. THE TRADING BOT ENGINE ---
class WallSpectrumLiveMonitor:
    def __init__(self, api_token):
        self.api_token = api_token
        self.ws_url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
        self.running = True
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
                "ticks_history": CONFIG["SYMBOL"], "start": start, "end": end, 
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
        LATEST_DATA["status"] = "Backfilling Data..."
        start_date = datetime.now(timezone.utc) - timedelta(days=CONFIG["LOOKBACK_DAYS"])
        print(f"[SYSTEM] Backfilling from {start_date}...")
        
        now = int(datetime.now(timezone.utc).timestamp())
        req_start = int(start_date.timestamp())
        
        tasks = []
        curr = req_start
        while curr < now:
            end = min(now, curr + 600)
            tasks.append((curr, end))
            curr = end

        # Download in parallel (Fast)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.fetch_ticks_chunk, t): t for t in tasks}
            for future in as_completed(futures):
                data = future.result()
                if data:
                    self.con.executemany("INSERT OR IGNORE INTO ticks VALUES (?, ?, ?)", data)
        
        print("[SYSTEM] Backfill Complete.")
        LATEST_DATA["status"] = "Active & Monitoring"

    def tick_collector(self):
        while self.running:
            try:
                ws = websocket.create_connection(self.ws_url)
                ws.send(json.dumps({"authorize": self.api_token}))
                ws.recv()
                ws.send(json.dumps({"ticks": CONFIG["SYMBOL"], "subscribe": 1}))
                while self.running:
                    data = json.loads(ws.recv())
                    if "tick" in data:
                        t = data["tick"]["epoch"]
                        p = data["tick"]["quote"]
                        self.con.execute("INSERT OR IGNORE INTO ticks VALUES (?, ?, ?)", (t, p, t*100000))
                        LATEST_DATA["price"] = p
            except:
                time.sleep(5)

    def analyze_logic(self):
        try:
            streak_kings = {}
            now_ts = int(time.time())
            start_ts = now_ts - (4 * 3600) 
            
            for cycle in range(CONFIG["MIN_CYCLE"], CONFIG["MAX_CYCLE"] + 1):
                for offset in range(cycle):
                    query = f"""
                        SELECT 
                            CAST(FLOOR((timestamp - {offset}) / {cycle}) AS BIGINT) * {cycle} + {offset} as bucket,
                            arg_min(price, sort_key) as open,
                            arg_max(price, sort_key) as close
                        FROM ticks
                        WHERE timestamp >= {start_ts}
                        GROUP BY bucket ORDER BY bucket ASC
                    """
                    candles = self.con.execute(query).fetchall()
                    if len(candles) < 5: continue

                    red_streaks = defaultdict(int)
                    green_streaks = defaultdict(int)
                    
                    prev_ts = candles[0][0]
                    c1_o, c1_c = candles[0][1], candles[0][2]
                    curr_color = 'Red' if c1_c < c1_o else ('Green' if c1_c > c1_o else 'Gray')
                    curr_streak = 1 if curr_color != 'Gray' else 0

                    for i in range(1, len(candles)):
                        ts, o, c = candles[i]
                        is_gap = (ts - prev_ts) > cycle
                        color = 'Red' if c < o else ('Green' if c > o else 'Gray')
                        
                        if is_gap or color == 'Gray':
                            if curr_streak > 0:
                                if curr_color == 'Red': red_streaks[curr_streak] += 1
                                elif curr_color == 'Green': green_streaks[curr_streak] += 1
                            curr_streak = 0
                            curr_color = 'Gray'
                            if not is_gap and color != 'Gray':
                                curr_color = color
                                curr_streak = 1
                        elif color == curr_color:
                            curr_streak += 1
                        else:
                            if curr_streak > 0:
                                if curr_color == 'Red': red_streaks[curr_streak] += 1
                                elif curr_color == 'Green': green_streaks[curr_streak] += 1
                            curr_color = color
                            curr_streak = 1
                        prev_ts = ts
                    
                    if curr_streak > 0:
                         if curr_color == 'Red': red_streaks[curr_streak] += 1
                         elif curr_color == 'Green': green_streaks[curr_streak] += 1

                    for col_name, s_dict in [('Red', red_streaks), ('Green', green_streaks)]:
                        for length, count in s_dict.items():
                            nxt = s_dict.get(length+1, 0)
                            if count == 0: continue
                            strength = (1 - (nxt/count)) * 100
                            
                            if CONFIG["MIN_STRENGTH"] <= strength <= CONFIG["MAX_STRENGTH"]:
                                key = (col_name, length)
                                if key not in streak_kings or strength > streak_kings[key]['strength']:
                                    streak_kings[key] = {
                                        "tf": f"C{cycle}_{offset:02d}",
                                        "color": col_name,
                                        "level": length,
                                        "curr": count,
                                        "next": nxt,
                                        "strength": round(strength, 2)
                                    }
            return sorted(streak_kings.values(), key=lambda x: (x['level'], x['color']))
        except Exception as e:
            print(f"Analysis Error: {e}")
            return []

    def run(self):
        self.backfill_data()
        threading.Thread(target=self.tick_collector, daemon=True).start()
        
        while self.running:
            kings = self.analyze_logic()
            LATEST_DATA["last_update"] = datetime.now().strftime("%H:%M:%S UTC")
            LATEST_DATA["kings"] = kings
            time.sleep(30) # Refresh analysis every 30s

# --- 2. WEB DASHBOARD ---
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>WallSpectrum Live</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: monospace; margin: 0; padding: 10px; }
        .header { border-bottom: 1px solid #30363d; padding-bottom: 10px; margin-bottom: 10px; }
        .top-row { display: flex; justify-content: space-between; align-items: center; }
        .price { font-size: 1.4em; font-weight: bold; color: #58a6ff; }
        .settings { font-size: 0.75em; color: #8b949e; margin-top: 5px; }
        .status-dot { height: 10px; width: 10px; background-color: #3fb950; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .status-warn { background-color: #d29922; }
        
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { text-align: left; color: #8b949e; border-bottom: 1px solid #30363d; padding: 5px; }
        td { padding: 8px 5px; border-bottom: 1px solid #21262d; }
        .red-row { border-left: 3px solid #ff7b72; background: rgba(255,123,114,0.05); }
        .green-row { border-left: 3px solid #3fb950; background: rgba(63,185,80,0.05); }
        .high-str { color: #f2cc60; font-weight: bold; }
    </style>
</head>
<body>
    <div class="header">
        <div class="top-row">
            <div>
                <span id="status-icon" class="status-dot status-warn"></span>
                <span id="sys-status">Connecting...</span>
            </div>
            <div class="price" id="price">---</div>
        </div>
        <div class="settings">
            SETTINGS: <span id="settings-text">...</span> <br>
            UPDATED: <span id="last-update">...</span>
        </div>
    </div>

    <div id="content">Loading Data...</div>

    <script>
        async function refresh() {
            try {
                let res = await fetch('/json');
                let data = await res.json();
                
                document.getElementById('price').innerText = data.price;
                document.getElementById('sys-status').innerText = data.status;
                document.getElementById('last-update').innerText = data.last_update;
                document.getElementById('settings-text').innerText = data.settings;
                
                let dot = document.getElementById('status-icon');
                if(data.status.includes("Active")) dot.className = "status-dot";
                else dot.className = "status-dot status-warn";

                let html = '<table><thead><tr><th>TF</th><th>Lvl</th><th>%</th><th>Ratio</th></tr></thead><tbody>';
                
                if (data.kings.length === 0) {
                    html += '<tr><td colspan="4" style="text-align:center; padding:20px;">No Walls Found (or Loading History...)</td></tr>';
                } else {
                    data.kings.forEach(k => {
                        let c = k.color === 'Red' ? 'red-row' : 'green-row';
                        let s = k.strength >= 90 ? 'high-str' : '';
                        html += `<tr class="${c}">
                            <td>${k.tf}</td>
                            <td>X${k.level}</td>
                            <td class="${s}">${k.strength}</td>
                            <td>${k.curr}/${k.next}</td>
                        </tr>`;
                    });
                }
                html += '</tbody></table>';
                document.getElementById('content').innerHTML = html;
            } catch (e) { console.log(e); }
        }
        setInterval(refresh, 5000);
        refresh();
    </script>
</body>
</html>
"""

@app.route('/')
def home(): return "Bot Online. <a href='/dashboard'>Dashboard</a>"

@app.route('/dashboard')
def dashboard(): return render_template_string(HTML_TEMPLATE)

@app.route('/json')
def get_json(): return jsonify(LATEST_DATA)

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))

if __name__ == "__main__":
    TOKEN = os.environ.get("DERIV_TOKEN", "TpVIBWpqet5X8AH")
    bot = WallSpectrumLiveMonitor(TOKEN)
    t = threading.Thread(target=bot.run)
    t.daemon = True
    t.start()
    run_flask()
