import os
import threading
from flask import Flask
from datetime import datetime, timezone, timedelta

# Import your original class here or paste the full class code above
# ... [PASTE YOUR FULL WallSpectrumLiveMonitor CLASS HERE] ...

# --- CLOUD SERVER SETUP ---
app = Flask(__name__)

@app.route('/')
def keep_alive():
    return "WallSpectrum Monitor is Running! 🟢"

def run_bot():
    # CLOUD CONFIGURATION (No User Input)
    API_TOKEN = os.environ.get("DERIV_TOKEN", "TpVIBWpqet5X8AH")
    
    # Auto-restart logic: Look back 2 days
    start_date = datetime.now(timezone.utc) - timedelta(days=2)
    
    print("Starting Background Worker...")
    bot = WallSpectrumLiveMonitor(
        api_token=API_TOKEN, 
        start_date=start_date, 
        min_c=60, max_c=300, 
        min_s=70, max_s=100
    )
    bot.run()

# --- ENTRY POINT ---
if __name__ == "__main__":
    # 1. Start the Trading Bot in a Background Thread
    t = threading.Thread(target=run_bot)
    t.daemon = True # Kills thread if main app crashes
    t.start()
    
    # 2. Start the Fake Web Server (Blocks the main thread to keep app alive)
    # Render provides a PORT env var. Default to 10000 locally.
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)