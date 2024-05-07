use std::collections::HashMap;

use redis::{aio::ConnectionManager, Value};

pub async fn create_connection(connection_url: &str) -> ConnectionManager {
    let redis_client = redis::Client::open(connection_url).expect("Failed to create redis client");
    ConnectionManager::new(redis_client)
        .await
        .expect("Failed to create redis connection")
}

pub async fn stream_events(
    stream_key: &str,
    handler: impl EventHandler,
    connection: ConnectionManager,
) {
    let save_key = &format!("events_api_websocket_last_id_{stream_key}");
    let mut db = redis_db::RedisDB::new(connection).await;
    let mut last_id = db.get(save_key).await.unwrap_or("$".to_string());
    log::info!("Last ID for {stream_key}: {last_id}");

    'outer: loop {
        let entries = db
            .xread(100, stream_key, &last_id) // will fetch up to 100 if running behind, or wait for the next 1 if not
            .await
            .expect("Failed to read redis stream");
        for (id, data) in entries {
            if let Err(err) = handler.handle(data).await {
                log::error!("Failed to handle event {id}: {err}");
                log::error!("Stopped reading events from {stream_key}");
                break 'outer;
            }

            last_id = id;
        }
        db.set(save_key, &last_id)
            .await
            .expect("Failed to set last ID");
    }
}

#[async_trait::async_trait]
pub trait EventHandler {
    async fn handle(&self, values: HashMap<String, Value>) -> anyhow::Result<()>;
}

// Modified version of https://github.com/fastnear/redis-node/blob/4b9eb42f5d22162fac22fa14e90481bc016483fa/src/bin/redis_db/mod.rs
mod redis_db {
    use std::{collections::HashMap, time::Duration};

    use itertools::Itertools;
    use redis::{aio::ConnectionManager, from_redis_value, Value};

    use self::stream::*;

    pub struct RedisDB {
        pub connection: ConnectionManager,
    }

    impl RedisDB {
        pub async fn new(connection: ConnectionManager) -> Self {
            Self { connection }
        }
    }

    impl RedisDB {
        pub async fn set(&mut self, key: &str, value: &str) -> redis::RedisResult<String> {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .query_async(&mut self.connection)
                .await
        }

        pub async fn get(&mut self, key: &str) -> redis::RedisResult<String> {
            redis::cmd("GET")
                .arg(key)
                .query_async(&mut self.connection)
                .await
        }

        pub async fn xread(
            &mut self,
            count: usize,
            key: &str,
            id: &str,
        ) -> redis::RedisResult<Vec<(String, HashMap<String, Value>)>> {
            let streams: Vec<Stream> = redis::cmd("XREAD")
                .arg("COUNT")
                .arg(count)
                .arg("BLOCK")
                .arg(Duration::from_millis(250).as_millis() as u64)
                .arg("STREAMS")
                .arg(key)
                .arg(id)
                .query_async(&mut self.connection)
                .await?;
            Ok(streams
                .into_iter()
                .filter(|s| s.id::<String>().unwrap() == key)
                .flat_map(|s| s.entries.into_iter())
                .map(|entry| {
                    let id = entry.id().unwrap();
                    let key_values = entry
                        .key_values
                        .into_iter()
                        .tuples()
                        .map(|(k, v)| (from_redis_value(&k).unwrap(), v))
                        .collect();
                    (id, key_values)
                })
                .collect())
        }
    }

    mod stream {
        use redis::{from_redis_value, FromRedisValue, RedisResult, Value};

        pub struct Stream {
            id: Value,
            pub entries: Vec<Entry>,
        }

        impl Stream {
            #[allow(dead_code)]
            pub fn id<RV: FromRedisValue>(&self) -> RedisResult<RV> {
                from_redis_value(&self.id)
            }
        }

        impl FromRedisValue for Stream {
            fn from_redis_value(v: &Value) -> RedisResult<Stream> {
                let (id, entries): (Value, Vec<Entry>) = from_redis_value(v)?;
                Ok(Stream { id, entries })
            }
        }

        pub struct Entry {
            id: Value,
            pub key_values: Vec<Value>,
        }

        impl FromRedisValue for Entry {
            fn from_redis_value(v: &Value) -> RedisResult<Entry> {
                let (id, key_values): (Value, Vec<Value>) = from_redis_value(v)?;
                Ok(Entry { id, key_values })
            }
        }

        impl Entry {
            pub fn id<RV: FromRedisValue>(&self) -> RedisResult<RV> {
                from_redis_value(&self.id)
            }
        }
    }
}
