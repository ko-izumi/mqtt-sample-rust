use anyhow::{Context, Result};
use std::{env, process, time::Duration};

extern crate paho_mqtt as mqtt;

use dotenv::dotenv;

const DFLT_BROKER: &str = "ssl://your-aws-endpoint.amazonaws.com:8883";
const DFLT_CLIENT: &str = "rust_publish";
const DFLT_TOPICS: &[&str] = &["rust/mqtt", "rust/test"];
// Define the qos.
const QOS: i32 = 1;

fn main() -> Result<()> {
    dotenv().ok();

    let host = env::var("BROKER").unwrap_or_else(|_| DFLT_BROKER.to_string());
    let trust_store =
        env::var("TRUST_STORE").context("TRUST_STORE environment variable not found")?;
    let key_store = env::var("KEY_STORE").context("KEY_STORE environment variable not found")?;
    let private_key =
        env::var("PRIVATE_KEY").context("PRIVATE_KEY environment variable not found")?;
    let client_id = env::var("CLIENT_ID").unwrap_or_else(|_| DFLT_CLIENT.to_string());

    println!("host: {}", host);
    println!("trust_store: {}", trust_store);
    println!("key_store: {}", key_store);
    println!("private_key: {}", private_key);
    println!("client_id: {}", client_id);

    // Define the set of options for the create.
    // Use an ID for a persistent session.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id(client_id)
        .finalize();

    println!("Client creating");

    // Create a client.
    let cli = mqtt::Client::new(create_opts).unwrap_or_else(|err| {
        println!("Error creating the client: {:?}", err);
        process::exit(1);
    });

    println!("Client created");

    // Define the set of options for the connection.
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .ssl_options(
            mqtt::SslOptionsBuilder::new()
                .trust_store(trust_store)?
                .key_store(key_store)?
                .private_key(private_key)?
                .finalize(),
        )
        .finalize();

    println!("Connecting...");

    // Connect and wait for it to complete or fail.
    if let Err(e) = cli.connect(conn_opts) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    println!("Connected to the broker");

    // Create a message and publish it.
    // Publish message to 'test' and 'hello' topics.
    for num in 0..5 {
        let content = "Hello world! ".to_string() + &num.to_string();
        let mut msg = mqtt::Message::new(DFLT_TOPICS[0], content.clone(), QOS);
        if num % 2 == 0 {
            println!("Publishing messages on the {:?} topic", DFLT_TOPICS[1]);
            msg = mqtt::Message::new(DFLT_TOPICS[1], content.clone(), QOS);
        } else {
            println!("Publishing messages on the {:?} topic", DFLT_TOPICS[0]);
        }
        let tok = cli.publish(msg);

        if let Err(e) = tok {
            println!("Error sending message: {:?}", e);
            break;
        }
    }

    // Disconnect from the broker.
    let tok = cli.disconnect(None);
    println!("Disconnect from the broker");
    tok.unwrap();

    Ok(())
}
