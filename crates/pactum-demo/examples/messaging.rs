//! Producer/consumer with a native outbox, fake lost ack, and durable inbox.
use pactum_messaging::{
    Dispatcher, FakeTransport, Fault, InboxStore, Message, OrderingPolicy, RetryPolicy,
};
use pactum_sqlite::messaging::SqliteMessaging;

fn error(e: impl Into<pactum_sqlite::AdapterError>) -> pactum_messaging::Error {
    pactum_messaging::Error::from_diagnostic(e.into().diagnostic())
}
fn increment(tx: &rusqlite::Transaction<'_>) -> Result<(), pactum_messaging::Error> {
    tx.execute(
        "INSERT INTO counter VALUES(1,1) ON CONFLICT(id) DO UPDATE SET value=value+1",
        [],
    )
    .map_err(error)?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "messaging-data".into());
    std::fs::create_dir_all(&directory)?;
    let mut producer = SqliteMessaging::open(std::path::Path::new(&directory).join("producer.db"))?;
    let mut consumer = SqliteMessaging::open(std::path::Path::new(&directory).join("consumer.db"))?;
    for store in [&producer, &consumer] {
        store.connection().execute_batch(
            "CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY,value INTEGER NOT NULL)",
        )?;
    }
    let message = Message {
        id: "account-created-1".into(),
        route: "accounts".into(),
        ordering_key: Some("account-1".into()),
        sequence: Some(1),
        version: 1,
        payload: br#"{"account":1}"#.to_vec(),
    };
    producer.produce(
        &message,
        RetryPolicy {
            backoff_ms: 0,
            max_backoff_ms: 0,
            ..RetryPolicy::default()
        },
        increment,
    )?;
    let mut transport = FakeTransport::default();
    transport.faults.extend([Fault::LoseAck, Fault::Duplicate]);
    for _ in 0..2 {
        let report = Dispatcher::default()
            .dispatch(&mut producer, &mut transport, || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            })
            .await?;
        println!("dispatch: {report:?}");
        while let Some(message) = transport.deliveries.pop_front() {
            let result = consumer.consume(
                "account-projection",
                &message,
                OrderingPolicy::Strict,
                3,
                increment,
            )?;
            println!("consume: {result:?}");
        }
    }
    for (name, store) in [("producer", &producer), ("consumer", &consumer)] {
        let count: i64 =
            store
                .connection()
                .query_row("SELECT value FROM counter WHERE id=1", [], |r| r.get(0))?;
        println!("{name} effects: {count}");
    }
    Ok(())
}
