use anyhow::Result;
use etcd_client::{
    Client, EventType, GetOptions, LeaderKey, ResignOptions, SortOrder, SortTarget, WatchResponse,
};
use log::*;
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{
    sync::{self, Mutex},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

type EtcdClient = Arc<Mutex<Client>>;

#[derive(thiserror::Error, Debug)]
pub enum OwnerError {
    #[error("no leader")]
    NoLeader,
    #[error("this node is not the owner")]
    NotOwner,
}

pub struct OwnerManager {
    campaign_name: String,
    id: String,
    client: EtcdClient,
    leader_ttl: i64,

    leader: Arc<Mutex<Option<etcd_client::LeaderKey>>>,
    cancel: Option<CancellationToken>,
}

struct EtcdSession {
    lease_id: i64,
    rx: sync::mpsc::Receiver<()>,
    cancel: CancellationToken,
}

impl EtcdSession {
    fn keep_alive_interval(ttl: i64) -> Duration {
        let interval_ms = 1000 * ttl as u64 * 2 / 3;
        Duration::from_millis(interval_ms)
    }

    pub fn get_lease_id(&self) -> i64 {
        self.lease_id
    }

    pub async fn new(client: EtcdClient, ttl: i64) -> Result<Self> {
        let lease_id = {
            let mut cli = client.lock().await;
            let resp = cli.lease_grant(ttl, None).await?;
            resp.id()
        };

        let (tx, rx) = sync::mpsc::channel(1);
        let cancel = CancellationToken::new();

        let interval = Self::keep_alive_interval(ttl);
        let cancel_keep_alive = cancel.clone();
        let task = async move {
            let _tx = tx;
            let (mut keeper, mut stream) = {
                let mut cli = client.lock().await;
                cli.lease_keep_alive(lease_id).await?
            };
            debug!("lease {:x} keep alive start", lease_id);
            let start = Instant::now();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => (),
                    _ = cancel_keep_alive.cancelled() => {
                        info!("etcd session is cancelled");
                        return Ok(())
                    }
                }
                let resp = keeper.keep_alive().await;
                if resp.is_err() {
                    return resp;
                }
                let resp = stream.message().await;
                if resp.is_err() {
                    return Err(resp.unwrap_err());
                }
                if let Some(r) = resp.unwrap() {
                    // the lease has been revoke or expired
                    if r.ttl() == 0 {
                        info!("etcd session is done");
                        break;
                    }
                    debug!("lease {:x} keep alive, new ttl {:?}", r.id(), r.ttl());
                    debug!("time elapsed:{:?}", start.elapsed());
                    // if start.elapsed() > Duration::from_secs(10) {
                    //     warn!("mock network delay");
                    //     tokio::time::sleep(interval).await;
                    // }
                    // continue next keep alive
                } else {
                    info!("lease {:x} keep alive failed, none", lease_id);
                    break;
                }
            }
            return Ok(());
        };

        tokio::spawn(async {
            if let Err(e) = task.await {
                error!("Error: while sending heartbeat. {:?}", e);
            }
        });

        Ok(Self { lease_id, rx, cancel, })
    }

    pub async fn recv(&mut self) -> Option<()> {
        self.rx.recv().await
    }

    pub fn cancel(&self) {
        self.cancel.cancel()
    }
}

pub async fn campaign(
    name: &str,
    id: &str,
    lease_id: i64,
    guard_client: EtcdClient,
) -> Result<LeaderKey> {
    info!("start campaign!");
    let mut client = guard_client.lock().await;
    let c = client.campaign(name, id, lease_id).await?;
    let leader = c.leader().unwrap().clone();
    Ok(leader)
}

fn is_key_invalid(msg: Option<WatchResponse>) -> bool {
    if msg.is_none() {
        info!("watcher is closed, no owner");
        return true;
    }
    let msg = msg.unwrap();
    if msg.canceled() {
        info!("watch canceled, no owner");
        return true;
    }

    for e in msg.events() {
        match e.event_type() {
            EventType::Delete => {
                info!("watch failed, owner is deleted");
                return true;
            }
            _ => continue,
        }
    }
    return false;
}

async fn get_owner_key(name: &str, expect_id: &str, client: EtcdClient) -> Result<Vec<u8>> {
    let mut client = client.lock().await;
    let leader = client.leader(name).await?;
    if leader.kv().is_none() {
        return Err(OwnerError::NoLeader.into());
    }
    let kv = leader.kv().unwrap();
    if kv.value_str()? != expect_id {
        info!("is not owner");
        return Err(OwnerError::NoLeader.into());
    }
    Ok(kv.key().to_owned())
}

async fn campaign_owner(
    name: String,
    id: String,
    ttl: i64,
    client: EtcdClient,
    leader_sender: sync::mpsc::Sender<Option<LeaderKey>>,
    cancel: CancellationToken,
) -> Result<()> {
    let client = client.clone();
    loop {
        let mut session = EtcdSession::new(client.clone(), ttl).await?;
        let lease_id = session.get_lease_id();

        let leader = campaign(&name, &id, lease_id, client.clone()).await?;
        let owner_key = get_owner_key(&name, &id, client.clone()).await?;

        // become owner
        leader_sender.send(Some(leader)).await?;

        let (_watcher, mut stream) = {
            let mut client = client.lock().await;
            client.watch(owner_key, None).await?
        };

        info!("start watching owner key");
        let mut cancelled = false;
        loop {
            tokio::select! {
                None = session.recv() => {
                    info!("session done!");
                    break;
                },
                Ok(msg) = stream.message() => {
                    if is_key_invalid(msg) {
                        info!("leader key is deleted");
                        break;
                    }
                }
                _ = cancel.cancelled() => {
                    // campaign is cancelled
                    info!("recv cancel");
                    cancelled = true;
                    break;
                }
            }
        }
        // retire leader
        leader_sender.send(None).await?;

        if cancelled {
            session.cancel();
            break;
        }
        // else start a new election
    }
    Ok(())
}

impl OwnerManager {
    pub fn new(campaign_name: &str, id: &str, client: EtcdClient) -> Self {
        Self {
            campaign_name: campaign_name.to_string(),
            id: id.to_string(),
            client,
            leader_ttl: 5,

            leader: Arc::new(Mutex::new(None)),
            cancel: None,
        }
    }

    pub async fn is_owner(&self) -> bool {
        let leader = self.leader.lock().await;
        leader.is_some()
    }

    pub async fn get_owner_id(&mut self) -> Result<String> {
        let opt = Some(
            GetOptions::new()
                .with_prefix()
                .with_sort(SortTarget::Version, SortOrder::Ascend)
                .with_limit(1),
        );
        let mut client = self.client.lock().await;
        let resp = client.get(self.campaign_name.clone(), opt).await?;
        if let Some(kv) = resp.kvs().first() {
            let value = kv.value_str()?;
            Ok(String::from_str(value)?)
        } else {
            Err(OwnerError::NoLeader.into())
        }
    }

    pub async fn campaign_owner(&mut self) -> Result<()> {
        let token = CancellationToken::new();
        let (tx, mut rx) = sync::mpsc::channel(1);
        {
            let cli = self.client.clone();
            let name = self.campaign_name.clone();
            let id = self.id.clone();
            let ttl = self.leader_ttl;
            let campaign_cancel = token.clone();
            tokio::spawn(
                async move { campaign_owner(name, id, ttl, cli, tx, campaign_cancel).await },
            );
        }

        {
            let leader = self.leader.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(resp) = rx.recv() => {
                            Self::set_leader(leader.clone(), resp).await;
                        }
                        else => {
                            break;
                        }
                    };
                }
            });
        }

        if let Some(old_cancel) = self.cancel.take() {
            old_cancel.cancel() 
        }
        let _ = self.cancel.insert(token);

        Ok(())
    }

    pub async fn resign_owner(&mut self) -> Result<()> {
        let mut leader = self.leader.lock().await;
        if let Some(l) = leader.take() {
            let mut client = self.client.lock().await;
            let opt = ResignOptions::new().with_leader(l);
            match client.resign(Some(opt)).await {
                Ok(_rsp) => {
                    info!("resign owner success");
                    return Ok(());
                }
                Err(e) => {
                    info!("resign error: {:?}", e);
                    return Err(e.into());
                }
            }
        } else {
            return Err(OwnerError::NotOwner.into());
        }
    }

    pub fn cancel(&self) {
        if let Some(c) = &self.cancel {
            c.cancel()
        }
    }

    // helper function to set/retire leader by `coming_msg`
    async fn set_leader(leader: Arc<Mutex<Option<LeaderKey>>>, coming_msg: Option<LeaderKey>) {
        let mut leader = leader.lock().await;
        if let Some(coming_leader) = coming_msg {
            info!("become owner, {:?}", coming_leader.key_str().unwrap());
            let _ = leader.insert(coming_leader);
        } else {
            let _ = leader.take();
            info!("retire owner");
        }
    }
}
