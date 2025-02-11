use std::{env, thread, time::Duration};

extern crate paho_mqtt as mqtt;

use anyhow::Result;
use dotenv::dotenv;

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

    // Initialize the consumer before connecting.
    let rx = cli.start_consuming();

    // Define the set of options for the connection.
    let lwt = mqtt::MessageBuilder::new()
        .topic("error")
        .payload("Consumer lost connection")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .ssl_options(ssl_opts)
        .clean_session(false)
        .will_message(lwt)
        .finalize();

    println!("connecting to the broker");

    // Connect and wait for it to complete or fail.
    cli.connect(conn_opts)?;

    println!("subscribing to the topics");

    // Subscribe topics.
    cli.subscribe(&client_id, 0)?;

    println!("Processing requests...");
    for msg in rx.iter() {
        if let Some(msg) = msg {
            println!("{}", msg);
        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                cli.subscribe(&client_id, 1)?;
            } else {
                break;
            }
        }
    }

    // If still connected, then disconnect now.
    if cli.is_connected() {
        println!("Disconnecting");
        cli.unsubscribe(&client_id)?;
        cli.disconnect(None)?;
    }
    println!("Exiting");

    Ok(())
}
