use std::{marker::PhantomData, time::Instant};

use actix::prelude::*;
use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws::WsResponseBuilder;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};

use crate::{
    EventFilter, EventWebSocket, FromRedis, Server, SubscribeToEvents, UnsubscribeFromEvents,
};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type NftTokenId = String;
type BlockHeight = u64;
type Balance = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct NftMintEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
}

#[derive(Debug, Serialize, Message)]
#[rtype(result = "()")]
pub struct FullNftMintEvent {
    #[serde(flatten)]
    pub event: NftMintEvent,
    #[serde(flatten)]
    pub context: NftEventContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftTransferEvent {
    pub old_owner_id: AccountId,
    pub new_owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
    pub token_prices_near: Vec<Option<Balance>>,
}

#[derive(Debug, Serialize, Message)]
#[rtype(result = "()")]
pub struct FullNftTransferEvent {
    #[serde(flatten)]
    pub event: NftTransferEvent,
    #[serde(flatten)]
    pub context: NftEventContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftBurnEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
}

#[derive(Debug, Serialize, Message)]
#[rtype(result = "()")]
pub struct FullNftBurnEvent {
    #[serde(flatten)]
    pub event: NftBurnEvent,
    #[serde(flatten)]
    pub context: NftEventContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftEventContext {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: String,
    pub contract_id: AccountId,
}

pub async fn nft_mint(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullNftMintEvent, NftMintFilter> {
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

impl FromRedis for FullNftMintEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftMintEvent>(&String::from_redis_value(
                values.get("mint").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullNftMintEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftMintFilter {
    owner_id: Option<AccountId>,
    contract_id: Option<AccountId>,
}

impl EventFilter<FullNftMintEvent> for NftMintFilter {
    fn matches(&self, event: &FullNftMintEvent) -> bool {
        if let Some(owner_id) = &self.owner_id {
            if event.event.owner_id != *owner_id {
                return false;
            }
        }

        if let Some(contract_id) = &self.contract_id {
            if event.context.contract_id != *contract_id {
                return false;
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullNftMintEvent, NftMintFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullNftMintEvent, NftMintFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_mint_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullNftMintEvent, NftMintFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullNftMintEvent, NftMintFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_mint_sockets.remove(&msg.0);
    }
}

pub async fn nft_transfer(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullNftTransferEvent, NftTransferFilter> {
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

impl FromRedis for FullNftTransferEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftTransferEvent>(&String::from_redis_value(
                values.get("transfer").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullNftTransferEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftTransferFilter {
    involved_account_ids: Option<Vec<AccountId>>,
    old_owner_id: Option<AccountId>,
    new_owner_id: Option<AccountId>,
    contract_id: Option<AccountId>,
}

impl EventFilter<FullNftTransferEvent> for NftTransferFilter {
    fn matches(&self, event: &FullNftTransferEvent) -> bool {
        if let Some(contract_id) = &self.contract_id {
            if event.context.contract_id != *contract_id {
                return false;
            }
        }

        if let Some(involved) = &self.involved_account_ids {
            if !involved.contains(&event.event.old_owner_id)
                && !involved.contains(&event.event.new_owner_id)
            {
                return false;
            }
        } else {
            if let Some(old_owner_id) = &self.old_owner_id {
                if event.event.old_owner_id != *old_owner_id {
                    return false;
                }
            }

            if let Some(new_owner_id) = &self.new_owner_id {
                if event.event.new_owner_id != *new_owner_id {
                    return false;
                }
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullNftTransferEvent, NftTransferFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullNftTransferEvent, NftTransferFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_transfer_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullNftTransferEvent, NftTransferFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullNftTransferEvent, NftTransferFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_transfer_sockets.remove(&msg.0);
    }
}

pub async fn nft_burn(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullNftBurnEvent, NftBurnFilter> {
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

impl FromRedis for FullNftBurnEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<NftEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<NftBurnEvent>(&String::from_redis_value(
                values.get("burn").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullNftBurnEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftBurnFilter {
    owner_id: Option<AccountId>,
    contract_id: Option<AccountId>,
}

impl EventFilter<FullNftBurnEvent> for NftBurnFilter {
    fn matches(&self, event: &FullNftBurnEvent) -> bool {
        if let Some(owner_id) = &self.owner_id {
            if event.event.owner_id != *owner_id {
                return false;
            }
        }

        if let Some(contract_id) = &self.contract_id {
            if event.context.contract_id != *contract_id {
                return false;
            }
        }

        true
    }
}

impl Handler<SubscribeToEvents<FullNftBurnEvent, NftBurnFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullNftBurnEvent, NftBurnFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_burn_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullNftBurnEvent, NftBurnFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullNftBurnEvent, NftBurnFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.nft_burn_sockets.remove(&msg.0);
    }
}
