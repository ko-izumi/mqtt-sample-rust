use anyhow::{Context, Result};
use std::{env, process, thread, time::Duration};

extern crate paho_mqtt as mqtt;

use dotenv::dotenv;

const DFLT_BROKER: &str = "ssl://your-aws-endpoint.amazonaws.com:8883";
const DFLT_CLIENT: &str = "rust_subscribe";
const DFLT_TOPICS: &[&str] = &["rust/mqtt", "rust/test"];
// The qos list that match topics above.
const DFLT_QOS: &[i32] = &[0, 1];

// Reconnect to the broker when connection is lost.
fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

// Subscribes to multiple topics.
fn subscribe_topics(cli: &mqtt::Client) {
    if let Err(e) = cli.subscribe_many(DFLT_TOPICS, DFLT_QOS) {
        println!("Error subscribes topics: {:?}", e);
        process::exit(1);
    }
}

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
    // Define the set of options for the create.
    // Use an ID for a persistent session.
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id(DFLT_CLIENT.to_string())
        .finalize();

    // Create a client.
    let cli = mqtt::Client::new(create_opts)?;

    // Initialize the consumer before connecting.
    let rx = cli.start_consuming();

    // Define the set of options for the connection.
    let lwt = mqtt::MessageBuilder::new()
        .topic("test")
        .payload("Consumer lost connection")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .will_message(lwt)
        .ssl_options(
            mqtt::SslOptionsBuilder::new()
                .trust_store("path/to/AmazonRootCA1.pem")?
                .key_store("path/to/your-private.pem.key")?
                .private_key("path/to/your-certificate.pem.crt")?
                .finalize(),
        )
        .finalize();

    // Connect and wait for it to complete or fail.
    if let Err(e) = cli.connect(conn_opts) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }

    // Subscribe topics.
    subscribe_topics(&cli);

    println!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            println!("{}", msg);
        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                subscribe_topics(&cli);
            } else {
                break;
            }
        }
    }

    // If still connected, then disconnect now.
    if cli.is_connected() {
        println!("Disconnecting");
        cli.unsubscribe_many(DFLT_TOPICS)?;
        cli.disconnect(None)?;
    }
    println!("Exiting");
    Ok(())
}
