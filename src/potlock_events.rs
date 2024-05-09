use std::{collections::HashMap, marker::PhantomData, time::Instant};

use actix::prelude::{dev::Message, Addr, Handler};
use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws::WsResponseBuilder;
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};

use crate::{
    AccountId, Balance, BlockHeight, DonationId, EventFilter, EventWebSocket, FromRedis, ProjectId,
    ReceiptId, Server, SubscribeToEvents, TimestampMs, TransactionId, UnsubscribeFromEvents,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockEventContext {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockDonationEvent {
    pub donation_id: DonationId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub account_id: AccountId,
    pub message: Option<String>,
    pub donated_at: TimestampMs,
    pub project_id: ProjectId,
    pub protocol_fee: Balance,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullPotlockDonationEvent {
    #[serde(flatten)]
    pub event: PotlockDonationEvent,
    #[serde(flatten)]
    pub context: PotlockEventContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotProjectDonationEvent {
    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub net_amount: Balance,
    pub message: Option<String>,
    pub donated_at: TimestampMs,
    pub project_id: ProjectId,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullPotlockPotProjectDonationEvent {
    #[serde(flatten)]
    pub event: PotlockPotProjectDonationEvent,
    #[serde(flatten)]
    pub context: PotlockEventContext,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotDonationEvent {
    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub net_amount: Balance,
    pub message: Option<String>,
    pub donated_at: TimestampMs,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: Option<Balance>,
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: Option<Balance>,
}

#[derive(Debug, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct FullPotlockPotDonationEvent {
    #[serde(flatten)]
    pub event: PotlockPotDonationEvent,
    #[serde(flatten)]
    pub context: PotlockEventContext,
}

pub async fn potlock_donation(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullPotlockDonationEvent, PotlockDonationEventFilter> {
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

impl FromRedis for FullPotlockDonationEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockDonationEvent>(&String::from_redis_value(
                values.get("donation").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullPotlockDonationEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockDonationEventFilter {
    pub project_id: Option<ProjectId>,
    pub donor_id: Option<AccountId>,
    pub referrer_id: Option<AccountId>,
    pub min_amounts: Option<HashMap<AccountId, Balance>>,
}

impl EventFilter<FullPotlockDonationEvent> for PotlockDonationEventFilter {
    fn matches(&self, event: &FullPotlockDonationEvent) -> bool {
        if let Some(project_id) = &self.project_id {
            if event.event.project_id != *project_id {
                return false;
            }
        }
        if let Some(donor_id) = &self.donor_id {
            if event.event.donor_id != *donor_id {
                return false;
            }
        }
        if let Some(referrer_id) = &self.referrer_id {
            if event.event.referrer_id.as_ref() != Some(referrer_id) {
                return false;
            }
        }
        if let Some(min_amounts) = &self.min_amounts {
            if let Some(min_amount) = min_amounts.get(&event.event.donor_id) {
                if let (Ok(total_amount), Ok(min_amount)) = (
                    event.event.total_amount.parse::<u128>(),
                    min_amount.parse::<u128>(),
                ) {
                    if total_amount < min_amount {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl Handler<SubscribeToEvents<FullPotlockDonationEvent, PotlockDonationEventFilter>> for Server {
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullPotlockDonationEvent, PotlockDonationEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_donation_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullPotlockDonationEvent, PotlockDonationEventFilter>>
    for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullPotlockDonationEvent, PotlockDonationEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_donation_sockets.remove(&msg.0);
    }
}

pub async fn potlock_pot_project_donation(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullPotlockPotProjectDonationEvent, PotlockPotProjectDonationEventFilter> {
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

impl FromRedis for FullPotlockPotProjectDonationEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockPotProjectDonationEvent>(&String::from_redis_value(
                values.get("pot_project_donation").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullPotlockPotProjectDonationEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotProjectDonationEventFilter {
    pub pot_id: Option<AccountId>,
    pub project_id: Option<ProjectId>,
    pub donor_id: Option<AccountId>,
    pub referrer_id: Option<AccountId>,
    pub min_amount_near: Option<Balance>,
}

impl EventFilter<FullPotlockPotProjectDonationEvent> for PotlockPotProjectDonationEventFilter {
    fn matches(&self, event: &FullPotlockPotProjectDonationEvent) -> bool {
        if let Some(pot_id) = &self.pot_id {
            if event.event.pot_id != *pot_id {
                return false;
            }
        }
        if let Some(project_id) = &self.project_id {
            if event.event.project_id != *project_id {
                return false;
            }
        }
        if let Some(donor_id) = &self.donor_id {
            if event.event.donor_id != *donor_id {
                return false;
            }
        }
        if let Some(referrer_id) = &self.referrer_id {
            if event.event.referrer_id.as_ref() != Some(referrer_id) {
                return false;
            }
        }
        if let Some(min_amount_near) = &self.min_amount_near {
            if let (Ok(total_amount), Ok(min_amount_near)) = (
                event.event.total_amount.parse::<u128>(),
                min_amount_near.parse::<u128>(),
            ) {
                if total_amount < min_amount_near {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl
    Handler<
        SubscribeToEvents<FullPotlockPotProjectDonationEvent, PotlockPotProjectDonationEventFilter>,
    > for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<
            FullPotlockPotProjectDonationEvent,
            PotlockPotProjectDonationEventFilter,
        >,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_pot_project_donation_sockets.insert(msg.0);
    }
}

impl
    Handler<
        UnsubscribeFromEvents<
            FullPotlockPotProjectDonationEvent,
            PotlockPotProjectDonationEventFilter,
        >,
    > for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<
            FullPotlockPotProjectDonationEvent,
            PotlockPotProjectDonationEventFilter,
        >,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_pot_project_donation_sockets.remove(&msg.0);
    }
}

pub async fn potlock_pot_donation(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<Addr<Server>>,
) -> Result<HttpResponse, Error> {
    let (addr, res) = WsResponseBuilder::new(
        EventWebSocket::<FullPotlockPotDonationEvent, PotlockPotDonationEventFilter> {
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

impl FromRedis for FullPotlockPotDonationEvent {
    fn from_redis(values: std::collections::HashMap<String, redis::Value>) -> anyhow::Result<Self> {
        match (
            serde_json::from_str::<PotlockEventContext>(&String::from_redis_value(
                values.get("context").unwrap(),
            )?),
            serde_json::from_str::<PotlockPotDonationEvent>(&String::from_redis_value(
                values.get("pot_donation").unwrap(),
            )?),
        ) {
            (Ok(context), Ok(event)) => Ok(FullPotlockPotDonationEvent { event, context }),
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotDonationEventFilter {
    pub pot_id: Option<AccountId>,
    pub donor_id: Option<AccountId>,
    pub referrer_id: Option<AccountId>,
    pub min_amount_near: Option<Balance>,
}

impl EventFilter<FullPotlockPotDonationEvent> for PotlockPotDonationEventFilter {
    fn matches(&self, event: &FullPotlockPotDonationEvent) -> bool {
        if let Some(pot_id) = &self.pot_id {
            if event.event.pot_id != *pot_id {
                return false;
            }
        }
        if let Some(donor_id) = &self.donor_id {
            if event.event.donor_id != *donor_id {
                return false;
            }
        }
        if let Some(referrer_id) = &self.referrer_id {
            if event.event.referrer_id.as_ref() != Some(referrer_id) {
                return false;
            }
        }
        if let Some(min_amount_near) = &self.min_amount_near {
            if let (Ok(total_amount), Ok(min_amount_near)) = (
                event.event.total_amount.parse::<u128>(),
                min_amount_near.parse::<u128>(),
            ) {
                if total_amount < min_amount_near {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl Handler<SubscribeToEvents<FullPotlockPotDonationEvent, PotlockPotDonationEventFilter>>
    for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: SubscribeToEvents<FullPotlockPotDonationEvent, PotlockPotDonationEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_pot_donation_sockets.insert(msg.0);
    }
}

impl Handler<UnsubscribeFromEvents<FullPotlockPotDonationEvent, PotlockPotDonationEventFilter>>
    for Server
{
    type Result = ();

    fn handle(
        &mut self,
        msg: UnsubscribeFromEvents<FullPotlockPotDonationEvent, PotlockPotDonationEventFilter>,
        _ctx: &mut Self::Context,
    ) {
        self.potlock_pot_donation_sockets.remove(&msg.0);
    }
}
