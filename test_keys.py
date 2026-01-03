import ccxt
import time

# ---------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------
API_KEY = 'akRu6fvija0EsU9cEp32Ij4pxepmMQ5nEu7NMb8cPaeu0nZCJlNelmkfctQuEpgX'
SECRET  = 'wfF3CfWE0djMG7VZDdJ7Feu60HtLmBovXsvylKWQGYQ8wqJeNpiImNHfswZG2GV1'

# Change to 'swap' for Futures Testnet, or 'spot' for Spot Testnet
MARKET_TYPE = 'spot' 
# ---------------------------------------------------

print(f"?? Connecting to Binance {MARKET_TYPE.upper()} Testnet...")

try:
    exchange = ccxt.binance({
        'apiKey': API_KEY,
        'secret': SECRET,
        'options': {'defaultType': MARKET_TYPE}
    })
    
    # ?? CRITICAL: Enable Sandbox/Testnet Mode
    exchange.set_sandbox_mode(True) 
    
    # Try to fetch balance (Requires valid API/Secret)
    balance = exchange.fetch_balance()
    
    print("\n? SUCCESS: API Key is working!")
    
    # Show some assets to confirm
    print("   Account Balances:")
    if MARKET_TYPE == 'spot':
        # Spot usually gives you BNB, BTC, BUSD, USDT, etc.
        for currency in ['BNB', 'BTC', 'USDT']:
            free = balance.get(currency, {}).get('free', 0)
            print(f"   - {currency}: {free}")
            
    elif MARKET_TYPE == 'swap':
        # Futures usually gives you USDT
        total_usdt = balance['total'].get('USDT', 0)
        print(f"   - USDT Equity: {total_usdt}")

except ccxt.AuthenticationError:
    print("\n? FAILURE: API Key or Secret is incorrect (401 Unauthorized).")
    print("   - Did you copy the key correctly?")
    print("   - Are you mixing up Spot keys with Futures keys?")
    
except ccxt.NetworkError as e:
    print(f"\n?? NETWORK ERROR: {e}")
    print("   - Check your internet connection.")
    print("   - The Testnet API might be down (it happens often).")
    
except Exception as e:
    print(f"\n? ERROR: {e}")