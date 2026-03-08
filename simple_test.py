#!/usr/bin/env python3
import sys
import os
print("Script started", flush=True)
print("Python version:", sys.version, flush=True)
print("Current dir:", os.getcwd(), flush=True)
try:
    print("Importing CryptoFetcher...", flush=True)
    from data_provider.crypto_fetcher import CryptoFetcher
    print("CryptoFetcher imported successfully", flush=True)
    fetcher = CryptoFetcher()
    print("CryptoFetcher instantiated successfully", flush=True)
    result = fetcher._is_crypto_code('BTC')
    print("BTC is crypto:", result, flush=True)
except Exception as e:
    print("Error:", e, flush=True)
    import traceback
    traceback.print_exc()
print("Script finished", flush=True)