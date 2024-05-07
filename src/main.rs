use std::{
    fs::File,
    io::BufReader,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use actix::prelude::*;
use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use actix_web_actors::ws;
use dashmap::DashSet;
use log::LevelFilter;
use nft_events::{
    FullNftBurnEvent, FullNftMintEvent, FullNftTransferEvent, NftBurnFilter, NftMintFilter,
    NftTransferFilter,
};
use redis::aio::ConnectionManager;
use redis_reader::{create_connection, stream_events, EventHandler};
use serde::{de::DeserializeOwned, Serialize};

mod nft_events;
mod redis_reader;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(15);

// EventWebSocket is the client, Server is the server.
// Typical flow:
// 1. EventWebSocket -> Server: SubscribeToEvents
// 2. Server: Adds the client to the list of subscribers
// 3. Server: Deserializes the Redis event using FromRedis trait
// 4. Server -> EventWebSocket: Event, Event, Event, ...
// 5. EventWebSocket: Checks if the event matches the filter using EventFilter trait and sends JSON-serialized event to the client
// 6. EventWebSocket -> Server: UnsubscribeFromEvents
// 7. Server: Removes the client from the list of subscribers

struct Server {
    redis_connection: ConnectionManager,
    nft_mint_sockets: Arc<DashSet<Addr<EventWebSocket<FullNftMintEvent, NftMintFilter>>>>,
    nft_transfer_sockets:
        Arc<DashSet<Addr<EventWebSocket<FullNftTransferEvent, NftTransferFilter>>>>,
    nft_burn_sockets: Arc<DashSet<Addr<EventWebSocket<FullNftBurnEvent, NftBurnFilter>>>>,
}

impl Actor for Server {
    type Context = actix::Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        tokio::spawn(stream_events(
            "nft_mint",
            SocketEventHandler(Arc::clone(&self.nft_mint_sockets)),
            self.redis_connection.clone(),
        ));
        tokio::spawn(stream_events(
            "nft_transfer",
            SocketEventHandler(Arc::clone(&self.nft_transfer_sockets)),
            self.redis_connection.clone(),
        ));
        tokio::spawn(stream_events(
            "nft_burn",
            SocketEventHandler(Arc::clone(&self.nft_burn_sockets)),
            self.redis_connection.clone(),
        ));
    }
}

pub struct EventWebSocket<E, F: EventFilter<E> + Unpin> {
    last_heartbeat: Instant,
    filter: Option<F>,
    server: Addr<Server>,
    _marker: PhantomData<E>,
}

pub trait EventFilter<E> {
    fn matches(&self, event: &E) -> bool;
}

struct SocketEventHandler<E: Unpin + 'static, F: EventFilter<E> + Unpin + 'static>(
    Arc<DashSet<Addr<EventWebSocket<E, F>>>>,
)
where
    Server: Handler<UnsubscribeFromEvents<E, F>>;

impl<E: Unpin + 'static, F: EventFilter<E> + Unpin + 'static> Actor for EventWebSocket<E, F>
where
    Server: Handler<UnsubscribeFromEvents<E, F>>,
{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.last_heartbeat = Instant::now();

        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.last_heartbeat) > CLIENT_TIMEOUT {
                ctx.stop();
            }

            ctx.ping(b"");
        });
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.server.do_send(UnsubscribeFromEvents(ctx.address()));
        Running::Stop
    }
}

pub trait FromRedis {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self>
    where
        Self: Sized;
}

#[async_trait::async_trait]
impl<E: Serialize + Send + Sync + FromRedis + Unpin, F: EventFilter<E> + Unpin> EventHandler
    for SocketEventHandler<E, F>
where
    Server: Handler<UnsubscribeFromEvents<E, F>>,
{
    async fn handle(
        &self,
        values: std::collections::HashMap<String, redis::Value>,
    ) -> anyhow::Result<()> {
        let event = Arc::new(Event(E::from_redis(values)?));
        for socket in self.0.iter() {
            socket.send(Arc::clone(&event)).await?;
        }
        Ok(())
    }
}

impl<E: Unpin + 'static, F: EventFilter<E> + DeserializeOwned + Unpin + 'static>
    StreamHandler<Result<ws::Message, ws::ProtocolError>> for EventWebSocket<E, F>
where
    Server: Handler<UnsubscribeFromEvents<E, F>>,
{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.last_heartbeat = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.last_heartbeat = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                if let Ok(filter) = serde_json::from_str::<F>(&text) {
                    self.filter = Some(filter);
                }
            }
            _ => ctx.stop(),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Event<E: Send>(E);

impl<E: Serialize + Send + Unpin + 'static, F: EventFilter<E> + Unpin + 'static>
    Handler<Arc<Event<E>>> for EventWebSocket<E, F>
where
    Server: Handler<UnsubscribeFromEvents<E, F>>,
{
    type Result = ();

    fn handle(&mut self, msg: Arc<Event<E>>, ctx: &mut Self::Context) -> Self::Result {
        if !self.filter.as_ref().map_or(true, |f| f.matches(&msg.0)) {
            return;
        }

        ctx.text(serde_json::to_string(&msg.0).unwrap());
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct SubscribeToEvents<E: Unpin + 'static, F: EventFilter<E> + Unpin + 'static>(
    Addr<EventWebSocket<E, F>>,
)
where
    Server: Handler<UnsubscribeFromEvents<E, F>>;

#[derive(Message)]
#[rtype(result = "()")]
struct UnsubscribeFromEvents<E: Unpin + 'static, F: EventFilter<E> + Unpin + 'static>(
    Addr<EventWebSocket<E, F>>,
)
where
    Server: Handler<UnsubscribeFromEvents<E, F>>;

#[actix::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let redis_connection = create_connection(
        &std::env::var("REDIS_URL").expect("REDIS_URL enviroment variable not set"),
    )
    .await;
    let server = Server {
        redis_connection,
        nft_mint_sockets: Arc::new(DashSet::new()),
        nft_transfer_sockets: Arc::new(DashSet::new()),
        nft_burn_sockets: Arc::new(DashSet::new()),
    };
    let server_addr = server.start();

    let tls_config = if let Ok(files) = std::env::var("SSL") {
        let mut certs_file = BufReader::new(File::open(files.split(',').nth(0).unwrap()).unwrap());
        let mut key_file = BufReader::new(File::open(files.split(',').nth(1).unwrap()).unwrap());
        let tls_certs = rustls_pemfile::certs(&mut certs_file)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let tls_key = rustls_pemfile::pkcs8_private_keys(&mut key_file)
            .next()
            .unwrap()
            .unwrap();
        Some(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(tls_certs, rustls::pki_types::PrivateKeyDer::Pkcs8(tls_key))
                .unwrap(),
        )
    } else {
        None
    };

    let server = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET"])
            .max_age(3600)
            .supports_credentials();

        let nft = web::scope("/nft")
            .service(web::resource("/nft_mint").route(web::get().to(nft_events::nft_mint)))
            .service(web::resource("/nft_transfer").route(web::get().to(nft_events::nft_transfer)))
            .service(web::resource("/nft_burn").route(web::get().to(nft_events::nft_burn)));

        let api_v0 = web::scope("/v0").service(nft);

        App::new()
            .app_data(web::Data::new(server_addr.clone()))
            .service(api_v0)
            .wrap(cors)
            .wrap(middleware::Logger::new(
                "%{r}a %a \"%r\"	Code: %s \"%{Referer}i\" \"%{User-Agent}i\" %T",
            ))
    });

    let server = if let Some(tls_config) = tls_config {
        server.bind_rustls_0_22(
            std::env::var("BIND_ADDRESS").unwrap_or("0.0.0.0:3000".to_string()),
            tls_config,
        )?
    } else {
        server.bind(std::env::var("BIND_ADDRESS").unwrap_or("0.0.0.0:3000".to_string()))?
    };

    server.run().await
}
