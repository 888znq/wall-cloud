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

# --- GLOBAL STORAGE (The Bridge between Bot and Web) ---
LATEST_DATA = {
    "status": "Initializing...",
    "last_update": "Waiting...",
    "price": 0,
    "kings": []
}

# --- 1. THE TRADING BOT ENGINE ---
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
        
        # In-Memory DB (Fast & Persistent for the session)
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
                        
                        # Update Live Price for UI
                        LATEST_DATA["price"] = p
            except:
                time.sleep(5)

    def analyze_logic(self):
        """Standard Enterprise Logic"""
        try:
            streak_kings = {}
            # Get data for analysis (Last 4 hours is enough for speed)
            now_ts = int(time.time())
            start_ts = now_ts - (4 * 3600) 
            
            for cycle in range(self.min_cycle, self.max_cycle + 1):
                for offset in range(cycle):
                    # DuckDB Flooring
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
                    if len(candles) < 10: continue

                    # Python Streak Logic
                    red_streaks = defaultdict(int)
                    green_streaks = defaultdict(int)
                    
                    prev_ts = candles[0][0]
                    # Determine first candle color
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
                            # Streak Ended
                            if curr_streak > 0:
                                if curr_color == 'Red': red_streaks[curr_streak] += 1
                                elif curr_color == 'Green': green_streaks[curr_streak] += 1
                            curr_color = color
                            curr_streak = 1
                        prev_ts = ts
                    
                    # Save final streak
                    if curr_streak > 0:
                         if curr_color == 'Red': red_streaks[curr_streak] += 1
                         elif curr_color == 'Green': green_streaks[curr_streak] += 1

                    # King Calculation
                    for col_name, s_dict in [('Red', red_streaks), ('Green', green_streaks)]:
                        for length, count in s_dict.items():
                            nxt = s_dict.get(length+1, 0)
                            if count == 0: continue
                            strength = (1 - (nxt/count)) * 100
                            
                            if self.min_strength <= strength <= self.max_strength:
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
            
            # Format for UI
            results = sorted(streak_kings.values(), key=lambda x: (x['level'], x['color']))
            return results

        except Exception as e:
            print(f"Analysis Error: {e}")
            return []

    def run(self):
        self.backfill_data()
        threading.Thread(target=self.tick_collector, daemon=True).start()
        
        while self.running:
            start_time = time.time()
            kings = self.analyze_logic()
            
            # UPDATE GLOBAL DATA
            LATEST_DATA["status"] = "Active"
            LATEST_DATA["last_update"] = datetime.now().strftime("%H:%M:%S UTC")
            LATEST_DATA["kings"] = kings
            
            print(f"[UPDATED] Found {len(kings)} Kings. Price: {LATEST_DATA['price']}")
            
            # Wait for next minute
            elapsed = time.time() - start_time
            sleep_time = max(1, 60 - elapsed)
            time.sleep(sleep_time)

# --- 2. THE WEB DASHBOARD (HTML/JS) ---
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>WallSpectrum Live</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: monospace; margin: 0; padding: 10px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #30363d; padding-bottom: 10px; margin-bottom: 10px; }
        .price-box { font-size: 1.2em; font-weight: bold; color: #58a6ff; }
        .status { font-size: 0.8em; color: #8b949e; }
        
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; color: #8b949e; border-bottom: 1px solid #30363d; padding: 5px; }
        td { padding: 8px 5px; border-bottom: 1px solid #21262d; }
        
        .red-row { border-left: 3px solid #ff7b72; }
        .green-row { border-left: 3px solid #3fb950; }
        .strength-high { color: #f2cc60; font-weight: bold; }
        
        .loading { text-align: center; margin-top: 50px; color: #8b949e; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div>WALLSPECTRUM <span style="color:red">LIVE</span></div>
            <div class="status" id="last-update">Waiting...</div>
        </div>
        <div class="price-box" id="price">---</div>
    </div>

    <div id="content">
        <div class="loading">Loading Data...</div>
    </div>

    <script>
        async function refreshData() {
            try {
                let response = await fetch('/json');
                let data = await response.json();
                
                document.getElementById('price').innerText = data.price;
                document.getElementById('last-update').innerText = "Last Scan: " + data.last_update;
                
                let html = '<table><thead><tr><th>TF</th><th>Lvl</th><th>Str</th><th>Ratio</th></tr></thead><tbody>';
                
                if (data.kings.length === 0) {
                    html += '<tr><td colspan="4" style="text-align:center; padding:20px;">No King Streaks Found</td></tr>';
                } else {
                    data.kings.forEach(k => {
                        let colorClass = k.color === 'Red' ? 'red-row' : 'green-row';
                        let strClass = k.strength > 90 ? 'strength-high' : '';
                        html += `<tr class="${colorClass}">
                            <td>${k.tf}</td>
                            <td>X${k.level}</td>
                            <td class="${strClass}">${k.strength}%</td>
                            <td>${k.curr}/${k.next}</td>
                        </tr>`;
                    });
                }
                html += '</tbody></table>';
                document.getElementById('content').innerHTML = html;
                
            } catch (e) {
                console.log("Error fetching data", e);
            }
        }
        
        // Refresh every 5 seconds
        setInterval(refreshData, 5000);
        refreshData();
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return "WallSpectrum Engine Online. Go to <a href='/dashboard'>/dashboard</a>"

@app.route('/dashboard')
def dashboard():
    return render_template_string(HTML_TEMPLATE)

@app.route('/json')
def get_json():
    return jsonify(LATEST_DATA)

# --- 3. ENTRY POINT ---
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    # CONFIGURATION
    TOKEN = os.environ.get("DERIV_TOKEN", "TpVIBWpqet5X8AH")
    S_DATE = datetime.now(timezone.utc) - timedelta(days=2) # 2 Days Lookback
    
    # Start Bot
    bot = WallSpectrumLiveMonitor(TOKEN, S_DATE, 60, 300, 70, 100)
    t = threading.Thread(target=bot.run)
    t.daemon = True
    t.start()
    
    # Start Web Server
    run_flask()
