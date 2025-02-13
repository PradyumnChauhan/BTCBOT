import requests
import json
from collections import defaultdict
import datetime
import pytz
from pymongo import MongoClient

# ------------------------------
# Configuration & Global Variables
# ------------------------------

# Telegram API configuration
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "trading_signals"

# Define Kolkata Timezone (IST)
IST = pytz.timezone("Asia/Kolkata")

# API Endpoints & Headers
URL_TICKERS = "https://cdn.india.deltaex.org/v2/tickers?contract_types=put_options,call_options,move_options"
URL_OI_CHANGE = "https://cdn.india.deltaex.org/v2/oi_change?asset_symbol=BTC&timeperiod=1h"
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.6",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not(A:Brand";v="99", "Brave";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "sec-gpc": "1",
    "Referer": "https://www.delta.exchange/",
    "Referrer-Policy": "strict-origin-when-cross-origin"
}

# ------------------------------
# Helper Functions for Data Processing
# ------------------------------

def parse_expiry(expiry_str):
    """
    Parse expiry string in DDMMYY format into a datetime object.
    Example: "280225" -> datetime(2025, 2, 28)
    """
    try:
        return datetime.datetime.strptime(expiry_str, "%d%m%y")
    except Exception as e:
        print(f"Error parsing expiry {expiry_str}: {e}")
        return None

def group_tickers_by_expiry_and_strike(tickers):
    """
    Group ticker data by expiry and strike.
    Expected ticker symbol format: "P-BTC-96000-280225" or "C-BTC-96000-280225"
    """
    grouped = defaultdict(lambda: defaultdict(dict))  # expiry -> strike -> { 'call': ..., 'put': ... }
    for ticker in tickers:
        symbol = ticker.get("symbol", "")
        parts = symbol.split('-')
        if len(parts) < 4:
            continue
        contract_code, asset, strike, expiry = parts[0], parts[1], parts[2], parts[3]
        if asset != "BTC":
            continue  # Skip non-BTC tickers
        ct = ticker.get("contract_type", "").lower()
        if "call" in ct:
            grouped[expiry][strike]["call"] = ticker
        elif "put" in ct:
            grouped[expiry][strike]["put"] = ticker
    return grouped

def merge_data(ticker_grouped, oi_change_data, taker_volume_data):
    """
    Merge tickers, OI change, and taker volume data into one JSON object.
    Data is grouped by expiry (first three sorted by date) then by strike.
    """
    expiries = set(ticker_grouped.keys()) | set(oi_change_data.keys()) | set(taker_volume_data.keys())
    sorted_expiries = sorted(
        [exp for exp in expiries if parse_expiry(exp) is not None],
        key=lambda x: parse_expiry(x)
    )
    # Limit to the first three expiries (adjust as needed)
    selected_expiries = sorted_expiries[:3]
    
    merged = {}
    for expiry in selected_expiries:
        merged[expiry] = {}
        strikes = set()
        if expiry in ticker_grouped:
            strikes |= set(ticker_grouped[expiry].keys())
        if expiry in oi_change_data:
            strikes |= set(oi_change_data[expiry].get("call", {}).keys())
            strikes |= set(oi_change_data[expiry].get("put", {}).keys())
        if expiry in taker_volume_data:
            strikes |= set(taker_volume_data[expiry].get("call_buy", {}).keys())
            strikes |= set(taker_volume_data[expiry].get("call_sell", {}).keys())
            strikes |= set(taker_volume_data[expiry].get("put_buy", {}).keys())
            strikes |= set(taker_volume_data[expiry].get("put_sell", {}).keys())
        
        merged[expiry]["strikes"] = {}
        for strike in strikes:
            merged[expiry]["strikes"][strike] = {
                "call": {
                    "ticker": ticker_grouped.get(expiry, {}).get(strike, {}).get("call"),
                    "oi_change": oi_change_data.get(expiry, {}).get("call", {}).get(strike),
                    "taker_volume": {
                        "buy": taker_volume_data.get(expiry, {}).get("call_buy", {}).get(strike),
                        "sell": taker_volume_data.get(expiry, {}).get("call_sell", {}).get(strike)
                    }
                },
                "put": {
                    "ticker": ticker_grouped.get(expiry, {}).get(strike, {}).get("put"),
                    "oi_change": oi_change_data.get(expiry, {}).get("put", {}).get(strike),
                    "taker_volume": {
                        "buy": taker_volume_data.get(expiry, {}).get("put_buy", {}).get(strike),
                        "sell": taker_volume_data.get(expiry, {}).get("put_sell", {}).get(strike)
                    }
                }
            }
    return merged

def fetch_tickers():
    """Fetch ticker data and filter for BTC tickers."""
    response = requests.get(URL_TICKERS, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        all_tickers = data.get("result", [])
        btc_tickers = [ticker for ticker in all_tickers if ticker.get("underlying_asset_symbol") == "BTC"]
        return btc_tickers
    else:
        print(f"Error fetching tickers: {response.status_code}")
        return None

def fetch_oi_change():
    """Fetch OI change data."""
    response = requests.get(URL_OI_CHANGE, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        return data.get("result", {})
    else:
        print(f"Error fetching OI change: {response.status_code}")
        return None

def fetch_taker_volume(start_time, end_time):
    """
    Fetch taker volume data using a dynamic time window.
    """
    url = f"https://cdn.india.deltaex.org/v2/options/taker_volume?start_time={start_time}&end_time={end_time}&underlying_asset=BTC"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        return data.get("result", {})
    else:
        print(f"Error fetching taker volume: {response.status_code}")
        return None

# ------------------------------
# Database & Telegram Helper Functions
# ------------------------------

def store_document(collection, document):
    """Store a document in the specified MongoDB collection."""
    try:
        result = collection.insert_one(document)
        return result.inserted_id
    except Exception as e:
        print(f"Error storing document: {e}")
        return None

def send_telegram_message(token, chat_id, message):
    """Send a message using the Telegram Bot API."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, data=data)
        return response.json()
    except Exception as e:
        print(f"Error sending Telegram message: {e}")
        return None

# ------------------------------
# Enhanced Signal Generation Function
# ------------------------------

def generate_signals_enhanced(current_data, previous_data, oi_threshold_pct=0.5, volume_threshold_pct=0.5, oi_abs_threshold=5000):
    """
    Generate consolidated signals by comparing current and previous merged data.
    For each expiry/strike/option type:
      - Compute the percentage change in OI and taker volume.
      - Generate a BUY signal for a call if both OI and taker volume (buy) have spiked.
      - Generate a SELL signal for a put if both OI and taker volume (sell) have spiked.
    Only generate a signal if the percentage increases exceed the given thresholds.
    """
    signals = []
    # Iterate over expiries in current data
    for expiry, exp_data in current_data.items():
        # Ensure previous data has this expiry
        if expiry not in previous_data:
            continue
        for strike, strike_data in exp_data.get("strikes", {}).items():
            # Ensure previous data has this strike
            prev_strike_data = previous_data.get(expiry, {}).get("strikes", {}).get(strike)
            if not prev_strike_data:
                continue
            for option in ["call", "put"]:
                current_opt = strike_data.get(option, {})
                previous_opt = prev_strike_data.get(option, {})
                current_oi = current_opt.get("oi_change")
                previous_oi = previous_opt.get("oi_change")
                if current_oi is None or previous_oi is None:
                    continue
                try:
                    current_oi = float(current_oi)
                    previous_oi = float(previous_oi)
                except:
                    continue
                if previous_oi == 0:
                    continue
                oi_pct_change = (current_oi - previous_oi) / abs(previous_oi)
                # For calls, consider taker_volume['buy'], for puts, consider taker_volume['sell']
                if option == "call":
                    current_vol = current_opt.get("taker_volume", {}).get("buy")
                    previous_vol = previous_opt.get("taker_volume", {}).get("buy")
                else:  # put
                    current_vol = current_opt.get("taker_volume", {}).get("sell")
                    previous_vol = previous_opt.get("taker_volume", {}).get("sell")
                if current_vol is None or previous_vol is None:
                    continue
                try:
                    current_vol = float(current_vol)
                    previous_vol = float(previous_vol)
                except:
                    continue
                if previous_vol == 0:
                    continue
                vol_pct_change = (current_vol - previous_vol) / abs(previous_vol)
                # Check if both OI and volume have spiked above thresholds
                if (oi_pct_change >= oi_threshold_pct and (current_oi - previous_oi) >= oi_abs_threshold and
                    vol_pct_change >= volume_threshold_pct):
                    signal_type = "BUY" if option == "call" else "SELL"
                    signals.append({
                        "expiry": expiry,
                        "strike": strike,
                        "option": option,
                        "signal": signal_type,
                        "oi_current": current_oi,
                        "oi_previous": previous_oi,
                        "oi_pct_change": oi_pct_change,
                        "vol_current": current_vol,
                        "vol_previous": previous_vol,
                        "vol_pct_change": vol_pct_change,
                        "reason": (f"{option.upper()} OI increased by {oi_pct_change*100:.1f}% (from {previous_oi} to {current_oi}) "
                                   f"and volume increased by {vol_pct_change*100:.1f}% (from {previous_vol} to {current_vol}).")
                    })
    return signals

# ------------------------------
# Data Aggregation Function
# ------------------------------

def create_merged_json():
    """
    Fetch and merge data from the tickers, OI change, and taker volume endpoints.
    Convert timestamps to IST and return the final JSON structure.
    """
    btc_tickers = fetch_tickers()
    if not btc_tickers:
        print("No BTC tickers available.")
        return None

    # Sort tickers by timestamp (latest first)
    btc_tickers.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    latest_ticker = btc_tickers[0]
    ticker_grouped = group_tickers_by_expiry_and_strike(btc_tickers)
    
    ticker_ts = latest_ticker.get("timestamp")
    if ticker_ts is None:
        print("Ticker timestamp not found!")
        return None

    # Convert from microseconds to seconds (Unix timestamp)
    end_time = int(ticker_ts / 1e6)
    start_time = end_time - 600  # 10-minute window

    # Convert Unix timestamps to IST datetime objects
    ticker_dt_utc = datetime.datetime.utcfromtimestamp(end_time)
    start_dt_utc = datetime.datetime.utcfromtimestamp(start_time)
    ticker_dt_ist = ticker_dt_utc.replace(tzinfo=pytz.utc).astimezone(IST)
    start_dt_ist = start_dt_utc.replace(tzinfo=pytz.utc).astimezone(IST)
    ticker_ts_formatted = ticker_dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    start_time_formatted = start_dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    end_time_formatted = ticker_ts_formatted  # end_time corresponds to the ticker timestamp

    # Fetch additional data
    oi_change_data = fetch_oi_change()
    if oi_change_data is None:
        return None
    taker_volume_data = fetch_taker_volume(start_time, end_time)
    if taker_volume_data is None:
        return None

    # Merge the data by expiry and strike
    merged = merge_data(ticker_grouped, oi_change_data, taker_volume_data)

    final_json = {
        "meta": {
            "ticker": {
                "raw_timestamp": ticker_ts,
                "timestamp_unix": end_time,
                "timestamp_formatted": ticker_ts_formatted
            },
            "oi_change": {
                "timeperiod": "1h"  # from the query parameter
            },
            "taker_volume": {
                "start_time_unix": start_time,
                "end_time_unix": end_time,
                "start_time_formatted": start_time_formatted,
                "end_time_formatted": end_time_formatted
            }
        },
        "data": merged
    }
    return final_json

# ------------------------------
# Main Workflow
# ------------------------------

def main():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    merged_collection = db.merged_data
    signals_collection = db.signals

    # Create current merged JSON data
    final_json = create_merged_json()
    if final_json is None:
        print("Error creating merged JSON.")
        return

    # Store current merged data into the database
    current_meta_ts = final_json["meta"]["ticker"]["timestamp_unix"]
    merged_id = store_document(merged_collection, final_json)
    print(f"Merged data stored with ID: {merged_id}")

    # Retrieve the most recent previous merged document (if any)
    previous_doc = merged_collection.find_one(
        {"meta.ticker.timestamp_unix": {"$lt": current_meta_ts}},
        sort=[("meta.ticker.timestamp_unix", -1)]
    )

    # If previous data exists, use it for enhanced signal generation
    if previous_doc and "data" in previous_doc:
        previous_data = previous_doc["data"]
        current_data = final_json.get("data", {})
        signals = generate_signals_enhanced(current_data, previous_data)
    else:
        print("No previous merged data available for comparison. No signals generated.")
        signals = []

    if signals:
        print("Enhanced signals generated:")
        for signal in signals:
            # Add a timestamp (in IST) for when the signal was generated
            signal["generated_at"] = datetime.datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            signal_id = store_document(signals_collection, signal)
            print(f"Signal stored with ID: {signal_id}")
            
            # Prepare a Telegram message
            message = (
                f"*Signal:* {signal['signal']}\n"
                f"*Expiry:* {signal['expiry']}\n"
                f"*Strike:* {signal['strike']}\n"
                f"*Option Type:* {signal['option'].upper()}\n"
                f"*Reason:* {signal['reason']}\n"
                f"*Time:* {signal['generated_at']}"
            )
            # Send the message via Telegram
            telegram_response = send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, message)
            print(f"Telegram response: {telegram_response}")
    else:
        print("No enhanced signals generated at this time.")

if __name__ == "__main__":
    main()
