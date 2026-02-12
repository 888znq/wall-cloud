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
from flask import Flask, jsonify, request, render_template_string

# --- DYNAMIC CONFIGURATION ---
CONFIG = {
    "SYMBOL": "frxEURUSD",
    "MIN_CYCLE": 60,  # User Default: 60
    "MAX_CYCLE": 60,  # User Default: 60
    "MIN_STRENGTH": 57, # User Default: 57
    "MAX_STRENGTH": 80  # User Default: 80
}

# --- GLOBAL STORAGE ---
LATEST_DATA = {
    "status": "Starting...",
    "last_update": "Waiting...",
    "price": 0,
    "kings": [],
    "config": CONFIG
}

# --- TRADING BOT ENGINE ---
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
        # --- NEW LOGIC: ALWAYS START FROM MONDAY 00:00 UTC ---
        now_utc = datetime.now(timezone.utc)
        # Calculate days since Monday (Monday=0, Sunday=6)
        days_since_monday = now_utc.weekday()
        # Subtract those days and reset time to 00:00:00
        monday_start = (now_utc - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        LATEST_DATA["status"] = f"Backfilling from Monday ({monday_start.strftime('%d %b')})..."
        print(f"[SYSTEM] Backfilling history from: {monday_start}")
        
        now_ts = int(now_utc.timestamp())
        req_start = int(monday_start.timestamp())
        
        tasks = []
        curr = req_start
        while curr < now_ts:
            end = min(now_ts, curr + 600)
            tasks.append((curr, end))
            curr = end

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
            # Analyze ONLY the data we have backfilled (Since Monday)
            now_ts = int(time.time())
            # Start analysis from 4 hours ago (Rolling window) or just analyze everything if needed
            # For speed, we usually keep analysis window smaller, but let's do 24h
            start_ts = now_ts - (24 * 3600) 
            
            for cycle in range(int(CONFIG["MIN_CYCLE"]), int(CONFIG["MAX_CYCLE"]) + 1):
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
                            
                            if float(CONFIG["MIN_STRENGTH"]) <= strength <= float(CONFIG["MAX_STRENGTH"]):
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
            LATEST_DATA["config"] = CONFIG
            time.sleep(10)

# --- WEB DASHBOARD ---
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>WallSpectrum Pro</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: monospace; margin: 0; padding: 10px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #30363d; padding-bottom: 10px; margin-bottom: 10px; }
        .price { font-size: 1.4em; font-weight: bold; color: #58a6ff; }
        .gear-btn { font-size: 1.5em; cursor: pointer; user-select: none; }
        .settings-panel { display: none; background: #161b22; padding: 15px; border: 1px solid #30363d; margin-bottom: 15px; border-radius: 6px; }
        .input-group { margin-bottom: 10px; }
        label { display: block; color: #8b949e; font-size: 0.8em; margin-bottom: 5px; }
        input { background: #0d1117; border: 1px solid #30363d; color: white; padding: 5px; width: 60px; font-family: monospace; }
        button { background: #238636; color: white; border: none; padding: 8px 15px; font-family: monospace; cursor: pointer; width: 100%; border-radius: 4px; }
        
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
        <div>
            <div>WALLSPECTRUM <span style="color:red">PRO</span></div>
            <div style="font-size:0.7em; color:#8b949e" id="status">Connecting...</div>
        </div>
        <div style="display:flex; align-items:center; gap:15px;">
            <div class="price" id="price">---</div>
            <div class="gear-btn" onclick="toggleSettings()">⚙️</div>
        </div>
    </div>

    <div id="settings-panel" class="settings-panel">
        <div style="display:flex; gap:10px;">
            <div class="input-group">
                <label>Min Cyc</label><input type="number" id="min_c">
            </div>
            <div class="input-group">
                <label>Max Cyc</label><input type="number" id="max_c">
            </div>
        </div>
        <div style="display:flex; gap:10px;">
            <div class="input-group">
                <label>Min %</label><input type="number" id="min_s">
            </div>
            <div class="input-group">
                <label>Max %</label><input type="number" id="max_s">
            </div>
        </div>
        <button onclick="saveSettings()">APPLY SETTINGS</button>
    </div>

    <div id="content">Loading Data...</div>

    <script>
        function toggleSettings() {
            let p = document.getElementById('settings-panel');
            p.style.display = p.style.display === 'block' ? 'none' : 'block';
        }

        async function saveSettings() {
            let data = {
                min_c: document.getElementById('min_c').value,
                max_c: document.getElementById('max_c').value,
                min_s: document.getElementById('min_s').value,
                max_s: document.getElementById('max_s').value
            };
            await fetch('/update_config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            toggleSettings();
            alert("Settings Updated!");
        }

        async function refresh() {
            try {
                let res = await fetch('/json');
                let data = await res.json();
                
                document.getElementById('price').innerText = data.price;
                document.getElementById('status').innerText = data.status;
                
                if(!document.getElementById('min_c').value && data.config) {
                    document.getElementById('min_c').value = data.config.MIN_CYCLE;
                    document.getElementById('max_c').value = data.config.MAX_CYCLE;
                    document.getElementById('min_s').value = data.config.MIN_STRENGTH;
                    document.getElementById('max_s').value = data.config.MAX_STRENGTH;
                }

                let html = '<table><thead><tr><th>TF</th><th>Lvl</th><th>%</th><th>Ratio</th></tr></thead><tbody>';
                if (data.kings.length === 0) {
                    html += '<tr><td colspan="4" style="text-align:center; padding:20px;">No Walls Found</td></tr>';
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

@app.route('/update_config', methods=['POST'])
def update_config():
    data = request.json
    CONFIG["MIN_CYCLE"] = int(data['min_c'])
    CONFIG["MAX_CYCLE"] = int(data['max_c'])
    CONFIG["MIN_STRENGTH"] = float(data['min_s'])
    CONFIG["MAX_STRENGTH"] = float(data['max_s'])
    LATEST_DATA["config"] = CONFIG
    return jsonify({"status": "ok"})

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))

if __name__ == "__main__":
    TOKEN = os.environ.get("DERIV_TOKEN", "TpVIBWpqet5X8AH")
    bot = WallSpectrumLiveMonitor(TOKEN)
    t = threading.Thread(target=bot.run)
    t.daemon = True
    t.start()
    run_flask()
