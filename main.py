from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from growwapi import GrowwAPI
import os
from dotenv import load_dotenv
from datetime import datetime, time, timedelta
import threading
import schedule
import time as time_module

# Load environment variables from .env file
load_dotenv()

# FastAPI initialization
app = FastAPI(
    title="Groww Stock Data API",
    description="API to fetch LTP and OHLC data for stock symbols",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend domain
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Define a Pydantic model for the input data (list of symbols)
class SymbolsRequest(BaseModel):
    symbols: list[str]

# API credentials (from .env file)
api_key = os.getenv("API_KEY")
secret = os.getenv("API_SECRET")

# Global variables for token management
access_token = None
groww = None
token_generated_date = None

def generate_access_token():
    """Generate a new access token and update the global groww client"""
    global access_token, groww, token_generated_date
    try:
        print(f"[{datetime.now()}] Generating new access token...")
        access_token = GrowwAPI.get_access_token(api_key=api_key, secret=secret)
        groww = GrowwAPI(access_token)
        token_generated_date = datetime.now().date()
        print(f"[{datetime.now()}] Access token generated successfully!")
        return access_token
    except Exception as e:
        print(f"[{datetime.now()}] Error generating access token: {e}")
        raise HTTPException(status_code=400, detail=f"Error fetching access token: {e}")

def should_regenerate_token():
    """Check if token should be regenerated based on time and date"""
    global token_generated_date
    
    current_time = datetime.now()
    current_date = current_time.date()
    
    # If no token exists, generate one
    if access_token is None or token_generated_date is None:
        return True
    
    # If it's a new day and current time is after 3:30 AM
    if current_date > token_generated_date:
        if current_time.time() >= time(3, 30):  # 3:30 AM
            return True
    
    # If it's the same day but we haven't generated token today after 3:30 AM
    elif current_date == token_generated_date:
        # Check if current time is after 3:30 AM and we generated token before 3:30 AM
        if current_time.time() >= time(3, 30):
            # If token was generated before today's 3:30 AM, regenerate
            today_330am = datetime.combine(current_date, time(3, 30))
            if datetime.combine(token_generated_date, time(0, 0)) < today_330am:
                return True
    
    return False

def get_valid_access_token():
    """Get a valid access token, regenerating if necessary"""
    global access_token, groww
    
    if should_regenerate_token():
        generate_access_token()
    
    return access_token, groww

def schedule_token_refresh():
    """Schedule daily token refresh at 3:30 AM"""
    schedule.every().day.at("03:30").do(generate_access_token)
    
    def run_scheduler():
        while True:
            schedule.run_pending()
            time_module.sleep(60)  # Check every minute
    
    # Run scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print(f"[{datetime.now()}] Token refresh scheduler started - will refresh daily at 3:30 AM")

# Initialize the access token and start the scheduler
try:
    generate_access_token()
    schedule_token_refresh()
except Exception as e:
    print(f"Failed to initialize: {e}")

# FastAPI route to get LTP for symbols
@app.post("/get-ltp")
async def get_ltp(request: SymbolsRequest):
    """
    Get Last Traded Price (LTP) for the provided stock symbols.
    
    Args:
        request: SymbolsRequest containing list of stock symbols
        
    Returns:
        dict: LTP data for each symbol
    """
    try:
        symbols = request.symbols  # List of symbols from the request body
        
        # Validate if symbols are provided
        if not symbols:
            raise HTTPException(status_code=400, detail="Symbols list cannot be empty")
        
        # Get valid access token and groww client
        current_token, current_groww = get_valid_access_token()
        
        # Fetch LTP for each symbol
        ltp_response = {}
        for symbol in symbols:
            ltp_data = current_groww.get_ltp(
                segment=current_groww.SEGMENT_CASH, exchange_trading_symbols=[symbol]
            )
            ltp_response[symbol] = ltp_data
        
        # Return the LTP data in the response
        return {"data": ltp_response}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching LTP data: {e}")

# FastAPI route to get OHLC for symbols
@app.post("/get-ohlc")
async def get_ohlc(request: SymbolsRequest):
    """
    Get OHLC (Open, High, Low, Close) data for the provided stock symbols.
    
    Args:
        request: SymbolsRequest containing list of stock symbols
        
    Returns:
        dict: OHLC data for each symbol
    """
    try:
        symbols = request.symbols  # List of symbols from the request body
        
        # Validate if symbols are provided
        if not symbols:
            raise HTTPException(status_code=400, detail="Symbols list cannot be empty")
        
        # Get valid access token and groww client
        current_token, current_groww = get_valid_access_token()
        
        # Fetch OHLC for each symbol
        ohlc_response = {}
        for symbol in symbols:
            ohlc_data = current_groww.get_ohlc(
                segment=current_groww.SEGMENT_CASH, exchange_trading_symbols=[symbol]
            )
            ohlc_response[symbol] = ohlc_data
        
        # Return the OHLC data in the response
        return {"data": ohlc_response}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching OHLC data: {e}")

# Token status endpoint for debugging
@app.get("/token-status")
async def token_status():
    """
    Get current token status and next refresh time.
    """
    global token_generated_date
    current_time = datetime.now()
    
    # Calculate next refresh time
    today_330am = datetime.combine(current_time.date(), time(3, 30))
    if current_time >= today_330am:
        next_refresh = datetime.combine(current_time.date() + timedelta(days=1), time(3, 30))
    else:
        next_refresh = today_330am
    
    return {
        "token_exists": access_token is not None,
        "token_generated_date": str(token_generated_date) if token_generated_date else None,
        "current_time": str(current_time),
        "next_refresh_time": str(next_refresh),
        "should_regenerate": should_regenerate_token()
    }

# Manual token refresh endpoint
@app.post("/refresh-token")
async def manual_refresh_token():
    """
    Manually refresh the access token.
    """
    try:
        generate_access_token()
        return {
            "message": "Token refreshed successfully",
            "timestamp": str(datetime.now()),
            "token_generated_date": str(token_generated_date)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing token: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)