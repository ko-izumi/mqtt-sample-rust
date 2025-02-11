use std::{env, time::Duration};

extern crate paho_mqtt as mqtt;

use anyhow::Result;
use dotenv::dotenv;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Payload {
    message: String,
    number: i32,
}

// Define the qos.
const QOS: i32 = 1;

fn main() -> Result<()> {
    dotenv().ok();
    let endpoint = env::var("ENDPOINT")?;
    let client_id = env::var("CLIENT_ID")?;

    let trust_store = env::var("TRUST_STORE")?;
    let key_store = env::var("KEY_STORE")?;
    let private_key = env::var("PRIVATE_KEY")?;

    // Define the set of options for the create.
    // Use an ID for a persistent session.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(endpoint)
        .client_id(client_id.clone())
        .finalize();

    // Create a client.
    let cli = mqtt::Client::new(create_opts)?;

    let ssl_opts = mqtt::SslOptionsBuilder::new()
        .trust_store(trust_store)?
        .key_store(key_store)?
        .private_key(private_key)?
        .finalize();

    // Define the set of options for the connection.
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .ssl_options(ssl_opts)
        .clean_session(true)
        .finalize();

    println!("connecting to the broker");

    // Connect and wait for it to complete or fail.
    cli.connect(conn_opts)?;

    println!("connected to the broker");

    // Create a message and publish it.
    // Publish message to 'test' and 'hello' topics.
    for num in 0..5 {
        let payload = Payload {
            message: "Hello world!".to_string(),
            number: num,
        };

        let payload_json = serde_json::to_string(&payload)?;

        let msg = mqtt::Message::new(client_id.clone(), payload_json, QOS);

        cli.publish(msg.clone())?;
        println!("published message: {:?}", msg.to_string());
    }

    // Disconnect from the broker.
    cli.disconnect(None)?;
    println!("Disconnect from the broker");

    Ok(())
}
