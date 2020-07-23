#![feature(async_closure)]

use tokio_amqp::*;
use lapin::{
    Consumer,
    Connection, 
    ConnectionProperties, 
    options::{
        BasicConsumeOptions,
        BasicAckOptions,
    }, 
    types::FieldTable,
    Channel,
    message::Delivery
};
use futures_util::stream::StreamExt;
use tracing::{info, error, Level, instrument};
use tracing_subscriber::FmtSubscriber;
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
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
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

    // connect to rabbitmq
    let mq_conn = Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?; 
    let mq_channel = mq_conn.create_channel().await?;

    // setup the rabbitmq consumer
    let mq_consumer = mq_channel.basic_consume(
        "hello", 
        "my_consumer", 
        BasicConsumeOptions::default(), 
        FieldTable::default(),
    ).await?;

    // consume messages from rabbitmq
    let mq_handle = tokio::spawn(consume_messages(mq_consumer));

    // start our rabbitmq consumer and the prometheus exporter
    let _ = tokio::join!(mq_handle, warp_handle);

    Ok(())
}

#[instrument]
async fn serve_metrics() -> Result<impl warp::Reply, Infallible> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buf).expect("encoding prometheus metrics as text");
    let cloned = &buf.clone();
    let text = from_utf8(cloned).expect("converting bytes to utf8");
    Ok(text.to_owned())
}

#[instrument]
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

#[instrument]
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

#[derive(Debug)]
enum Error {
    IoError(std::io::Error),
    Utf8Error(std::str::Utf8Error),
    PrometheusError(Box<prometheus::Error>),
    WarpError(warp::Error),
    LapinError(lapin::Error),
    JoinError(tokio::task::JoinError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoError(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Error {
        Error::Utf8Error(e)
    }
}

impl From<prometheus::Error> for Error {
    fn from(e: prometheus::Error) -> Error {
        Error::PrometheusError(Box::new(e))
    }
}

impl From<warp::Error> for Error {
    fn from(e: warp::Error) -> Error {
        Error::WarpError(e)
    }
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Error {
        Error::LapinError(e)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Error {
        Error::JoinError(e)
    }
}
