import os
import json
import logging
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

CONNECT_HOST = os.getenv("CONNECT_HOST", "http://localhost:8083")
TEMPLATE_FILE = Path(__file__).resolve().parents[1] / 'sinl_connector.template.json'


def _read_template():
    with open(TEMPLATE_FILE, 'r', encoding='utf-8') as fh:
        return fh.read()


def _render_template(tpl: str, env: dict) -> dict:
    for key, value in env.items():
        tpl = tpl.replace('${' + key + '}', value or '')
    return json.loads(tpl)


def _format_private_key(key: str) -> str:
    if not key:
        return ''
    # Strip quotes if present (e.g. from .env load)
    key = key.strip().strip('"')
    
    # If header missing, add it
    if not key.startswith("-----BEGIN PRIVATE KEY-----"):
        key = f"-----BEGIN PRIVATE KEY-----\n{key}\n-----END PRIVATE KEY-----"
        
    return key.replace('\n', '\\n')


def main():
    env_map = {
        'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', ''),
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT', ''),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER', ''),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER', ''),
        # PRIVATE_KEY may contain newlines; keep as-is for JSON
        'PRIVATE_KEY': _format_private_key(os.getenv('PRIVATE_KEY', '')),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE', 'dev_raw_db'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA', 'kafka'),
    }

    tpl = _read_template()
    payload = _render_template(tpl, env_map)

    connector_name = payload.get('name')
    url = f"{CONNECT_HOST}/connectors"
    logging.info(f"Registering connector {connector_name} at {url}")
    # print(json.dumps(payload, indent=2)) # Debug

    r = requests.post(url, json=payload)
    if r.status_code in (200, 201):
        logging.info('Connector created or updated successfully')
        print(r.json())
    else:
        logging.error(f"Failed to register connector: {r.status_code} {r.text}")
        r.raise_for_status()


if __name__ == '__main__':
    main()
