import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()

def _format_private_key(key: str) -> str:
    if not key:
        return ''
    key = key.strip().strip('"')
    if not key.startswith("-----BEGIN PRIVATE KEY-----"):
        key = f"-----BEGIN PRIVATE KEY-----\n{key}\n-----END PRIVATE KEY-----"
    return key

def verify():
    raw_key = os.getenv("PRIVATE_KEY")
    if not raw_key:
        print("PRIVATE_KEY not found in .env")
        return

    formatted_key = _format_private_key(raw_key)
    print("Formatted Key (first 50 chars):", formatted_key[:50])
    
    try:
        serialization.load_pem_private_key(
            formatted_key.encode('utf-8'),
            password=None,
            backend=default_backend()
        )
        print("SUCCESS: Private key is valid.")
    except Exception as e:
        print(f"FAILURE: Private key is invalid. Error: {e}")

if __name__ == "__main__":
    verify()
