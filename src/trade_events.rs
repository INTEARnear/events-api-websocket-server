use std::{collections::HashMap, marker::PhantomData, time::Instant};

use actix::prelude::{dev::Message, Addr, Handler};
use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws::WsResponseBuilder;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};

use crate::{
    AccountId, Balance, BlockHeight, EventFilter, EventWebSocket, FromRedis, PoolId, ReceiptId,
    Server, SubscribeToEvents, TransactionId, UnsubscribeFromEvents,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeContext {
    pub trader: AccountId,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: Balance,
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawPoolSwap {
    pub pool: PoolId,
    pub token_in: AccountId,
    pub token_out: AccountId,
    pub amount_in: Balance,
    pub amount_out: Balance,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullTradePoolEvent {
    #[serde(flatten)]
    pub event: RawPoolSwap,
    #[serde(flatten)]
    pub context: TradeContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeBalanceChangeSwap {
    pub balance_changes: HashMap<AccountId, Balance>,
    pub pool_swaps: Vec<RawPoolSwap>,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullTradeSwapEvent {
    #[serde(flatten)]
    pub event: TradeBalanceChangeSwap,
    #[serde(flatten)]
    pub context: TradeContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolChangeEvent {
    pub pool_id: PoolId,
    pub receipt_id: ReceiptId,
    pub block_timestamp_nanosec: String,
    pub block_height: BlockHeight,
    pub pool: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullTradePoolChangeEvent {
    #[serde(flatten)]
    pub event: TradePoolChangeEvent,
}

pub async fn trade_pool(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullTradePoolEvent, TradePoolEventFilter> {
            last_heartbeat: Instant::now(),
            filter: None,
            server: server.get_ref().clone(),
            _marker: PhantomData,
        },
        &req,
        stream,
    )
    .start_with_addr()?;
    server.send(SubscribeToEvents(addr)).await.unwrap();
    Ok(res)
}

impl FromRedis for FullTradePoolEvent {
    fn from_redis(values: HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<TradeContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<RawPoolSwap>(&String::from_redis_value(
                values.get("swap").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullTradePoolEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolEventFilter {
    pool_id: Option<PoolId>,
    account_id: Option<AccountId>,
}

impl EventFilter<FullTradePoolEvent> for TradePoolEventFilter {
    fn matches(&self, event: &FullTradePoolEvent) -> bool {
        if let Some(pool_id) = &self.pool_id {
            if event.event.pool != *pool_id {
                return false;
            }
        }

        if let Some(account_id) = &self.account_id {
            if event.context.trader != *account_id {
                return false;
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullTradePoolEvent, TradePoolEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullTradePoolEvent, TradePoolEventFilter>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.trade_pool_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullTradePoolEvent, TradePoolEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullTradePoolEvent, TradePoolEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.trade_pool_sockets.remove(&msg.0);
    }
}

pub async fn trade_swap(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullTradeSwapEvent, TradeSwapEventFilter> {
            last_heartbeat: Instant::now(),
            filter: None,
            server: server.get_ref().clone(),
            _marker: PhantomData,
        },
        &req,
        stream,
    )
    .start_with_addr()?;
    server.send(SubscribeToEvents(addr)).await.unwrap();
    Ok(res)
}

impl FromRedis for FullTradeSwapEvent {
    fn from_redis(values: HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<TradeContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<TradeBalanceChangeSwap>(&String::from_redis_value(
                values.get("balance_change").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullTradeSwapEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSwapEventFilter {
    account_id: Option<AccountId>,
    involved_token_account_ids: Option<Vec<AccountId>>,
}

impl EventFilter<FullTradeSwapEvent> for TradeSwapEventFilter {
    fn matches(&self, event: &FullTradeSwapEvent) -> bool {
        if let Some(account_id) = &self.account_id {
            if event.context.trader != *account_id {
                return false;
            }
        }

        if let Some(involved_token_account_ids) = &self.involved_token_account_ids {
            for involved_token in involved_token_account_ids {
                if !event.event.balance_changes.contains_key(involved_token)
                    || event.event.balance_changes.get(involved_token).unwrap() == "0"
                {
                    return false;
                }
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullTradeSwapEvent, TradeSwapEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullTradeSwapEvent, TradeSwapEventFilter>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.trade_swap_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullTradeSwapEvent, TradeSwapEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullTradeSwapEvent, TradeSwapEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.trade_swap_sockets.remove(&msg.0);
    }
}

pub async fn trade_pool_change(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullTradePoolChangeEvent, TradePoolChangeEventFilter> {
            last_heartbeat: Instant::now(),
            filter: None,
            server: server.get_ref().clone(),
            _marker: PhantomData,
        },
        &req,
        stream,
    )
    .start_with_addr()?;
    server.send(SubscribeToEvents(addr)).await.unwrap();
    Ok(res)
}

impl FromRedis for FullTradePoolChangeEvent {
    fn from_redis(values: HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match serde_json::from_str::<TradePoolChangeEvent>(&String::from_redis_value(
            values.get("pool_change").unwrap(),
        )?) {
            Ok(event) => Ok(FullTradePoolChangeEvent { event }),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolChangeEventFilter {
    pool_id: Option<PoolId>,
}

impl EventFilter<FullTradePoolChangeEvent> for TradePoolChangeEventFilter {
    fn matches(&self, event: &FullTradePoolChangeEvent) -> bool {
        if let Some(pool_id) = &self.pool_id {
            if event.event.pool_id != *pool_id {
                return false;
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullTradePoolChangeEvent, TradePoolChangeEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullTradePoolChangeEvent, TradePoolChangeEventFilter>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.trade_pool_change_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullTradePoolChangeEvent, TradePoolChangeEventFilter>>
    for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullTradePoolChangeEvent, TradePoolChangeEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.trade_pool_change_sockets.remove(&msg.0);
    }
}
