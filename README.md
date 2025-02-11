# MQTT Client for AWS IoT

## Environment Variables

You need to set the following environment variables to run the client. `.env` file is required in the root directory.

- `ENDPOINT`: MQTT broker endpoint
- `TRUST_STORE`: Path to the trust store
- `KEY_STORE`: Path to the key store
- `PRIVATE_KEY`: Path to the private key

example:

```bash
ENDPOINT=ssl://xxxxxx-ats.iot.xxxxxx.amazonaws.com:8883
TRUST_STORE=assets/AmazonRootCA1.pem
KEY_STORE=assets/xxxxxxxxxxxx-certificate.pem.crt
PRIVATE_KEY=assets/xxxxxxxxxxxx-private.pem.key
```

## Build

```bash
cargo build
```

## Publish

```bash
cargo run --bin pub
```

## Subscribe

```bash
cargo run --bin sub
```

## referenced links

- [paho-mqtt](https://github.com/eclipse/paho.mqtt.rust)
- [qiita](https://qiita.com/emqx_japan/items/3309893c832b45cc2ec5)