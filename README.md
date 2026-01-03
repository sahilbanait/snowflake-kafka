Quick start

Prereqs: Docker & Docker Compose (or the ability to run the included Redpanda image)

1. Start local Redpanda & console:

```powershell
docker compose up -d
```

2. Verify Redpanda is available on localhost:19092. The project uses REDPANDA_BROKERS=127.0.0.1:19092 by default (see `.env`).

3. Produce 1 test message and publish it to the topic (example):

```powershell
# generate JSON messages then publish
python ./data_generator.py 1 | python ./publish_data.py
```

If you see connection errors like "socket disconnected", confirm Redpanda is running and reachable at the `REDPANDA_BROKERS` address in your `.env`.

## Snowflake connector setup

This project includes a Kafka Connect image and a template for the Snowflake Sink Connector. To connect Redpanda -> Kafka Connect -> Snowflake, do the following:

1. Update `.env` with your Snowflake credentials (these are the keys used by the example config):

```powershell
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_user
PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
KAFKA_TOPIC=lift_tickets
SNOWFLAKE_DATABASE=dev_raw_db
SNOWFLAKE_SCHEMA=kafka
```

2. Build and start the stack (the Dockerfile adds the Snowflake connector jars into the Connect image):

```powershell
docker compose build connect
docker compose up -d
```

3. Register the connector (the repository includes a template `sinl_connector.template.json` and a helper script to POST it):

```powershell
# ensure the project has the small helper deps installed
pip install requests python-dotenv
python ./scripts/register_snowflake_connector.py
```

4. Verify the connector is registered:

```powershell
curl http://localhost:8083/connectors
```

Notes:

- The Dockerfile places the required connector jars into the Connect plugin directory at `/opt/kafka/connect-plugins/snowflake`. If you change CONNECT_PLUGIN_PATH update the Dockerfile accordingly.
- The connector template uses env vars and the register script will substitute values from your `.env` file.
