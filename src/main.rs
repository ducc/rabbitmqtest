#![feature(async_closure)]

mod error;

use error::Error;

use tokio_amqp::*;
use lapin::{
    Consumer,
    Connection, 
    ConnectionProperties, 
    options::{
        BasicConsumeOptions,
        BasicAckOptions,
        QueueDeclareOptions,
    }, 
    types::FieldTable,
    Channel,
    message::Delivery
};
use futures_util::stream::StreamExt;
use tracing::{info, error};
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use std::{
    str::from_utf8,
    net::SocketAddr,
    convert::Infallible,
    vec::Vec,
};
use prometheus::{self, IntCounter, register_int_counter, TextEncoder, Encoder};
use lazy_static::lazy_static;
use warp::Filter;

// initialize the prometheus metrics
lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTER: IntCounter = register_int_counter!(
        "rabbitmq_messages_received", 
        "Number of messages received from RabbitMQ"
    ).unwrap();

    static ref MESSAGES_ACKED_COUNTER: IntCounter = register_int_counter!(
        "rabbitmq_messages_acked", 
        "Number of messages acked from RabbitMQ"
    ).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup our logging and tracing
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("unable to set default trace subscriber");

    // get the prometheus server address
    let prom_addr_raw = std::env::var("PROMETHEUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let prom_addr: SocketAddr = prom_addr_raw.parse().expect("cannot parse prometheus address");

    // start the prometheus server
    let exporter = warp::path!("metrics").and_then(serve_metrics);
    let warp_handle = warp::serve(exporter).run(prom_addr);

    // get the rabbitmq address
    let mq_addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    info!(address = &mq_addr[..], "connecting to rabbitmq");

    // get the queue name
    let queue_name = std::env::var("AMQP_QUEUE").expect("queue name is required");

    // connect to rabbitmq
    let mq_conn = Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?; 

    // declare the rabbitmq queue
    let mq_declare_channel = mq_conn.create_channel().await?;
    let _ = mq_declare_channel.queue_declare(
        &queue_name,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    ).await?;

    // setup the rabbitmq consumer
    let mq_channel = mq_conn.create_channel().await?;
    let mq_consumer = mq_channel.basic_consume(
        &queue_name, 
        "", 
        BasicConsumeOptions::default(), 
        FieldTable::default(),
    ).await?;

    // consume messages from rabbitmq
    let mq_handle = tokio::spawn(consume_messages(mq_consumer));

    // start our rabbitmq consumer and the prometheus exporter
    let _ = tokio::join!(mq_handle, warp_handle);

    Ok(())
}

async fn serve_metrics() -> Result<impl warp::Reply, Infallible> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buf).expect("encoding prometheus metrics as text");
    let text = from_utf8(&buf).expect("converting bytes to utf8");
    Ok(text.to_owned())
}

async fn consume_messages(mut mq_consumer: Consumer) {
    while let Some(delivery) = mq_consumer.next().await {
        let (channel, delivery) = match delivery {
            Ok(v) => v,
            Err(e) => {
                error!("unable to get delivery from rabbitmq consumer: {}", e);
                return;
            },
        };

        handle_delivery(channel, delivery).await;
    }
}

async fn handle_delivery(channel: Channel, delivery: Delivery) {
    MESSAGES_RECEIVED_COUNTER.inc();

    // when a message arrives we convert the binary data into utf8 text
    let data = match from_utf8(&delivery.data) {
        Ok(v) => v,
        Err(e) => {
            error!("unable to convert message data into utf8 string: {}", e);
            return;
        },
    };
    info!("{}", data);

    // if we were able to decode the data, ack the message to say it has been completed
    if let Err(e) = channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await {
        error!("unable to ack message: {}", e);
        return;
    }

    MESSAGES_ACKED_COUNTER.inc();
}

