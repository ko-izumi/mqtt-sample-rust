use std::{thread, time::Duration};

extern crate paho_mqtt as mqtt;

use anyhow::Result;
use dotenv::dotenv;

const DFLT_BROKER: &str = "tcp://broker.emqx.io:1883";
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

fn main() -> Result<()> {
    dotenv().ok();
    let host = DFLT_BROKER.to_string();

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
        .finalize();

    // Connect and wait for it to complete or fail.
    cli.connect(conn_opts)?;

    // Subscribe topics.
    cli.subscribe_many(DFLT_TOPICS, DFLT_QOS)?;

    println!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            println!("{}", msg);
        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                cli.subscribe_many(DFLT_TOPICS, DFLT_QOS)?;
            } else {
                break;
            }
        }
    }

    // If still connected, then disconnect now.
    if cli.is_connected() {
        println!("Disconnecting");
        cli.unsubscribe_many(DFLT_TOPICS).unwrap();
        cli.disconnect(None).unwrap();
    }
    println!("Exiting");

    Ok(())
}
