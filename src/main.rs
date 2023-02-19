use std::{env::args, sync::Arc, time::Duration};

mod manager;

use anyhow::Result;
use log::*;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let id = args().nth(1).expect("id not set!");
    info!("id={}", &id);

    let client = Arc::new(Mutex::new(
        etcd_client::Client::connect(["172.16.5.85:6520"], None).await?,
    ));

    let name = "/tiflash/s3gc/owner";

    let mut mgr = manager::OwnerManager::new(&name, &id, client);
    info!("mgr init done");
    mgr.campaign_owner().await?;
    info!("mgr campaign begin");

    for x in 0..10 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!(
            "processing, is_owner={} owner_id={}",
            mgr.is_owner().await,
            mgr.get_owner_id().await?
        );
        if x == 1 {
            info!("resign owner");
            mgr.resign_owner().await?;
        }
        else if x == 2 {
            info!("cancel");
            mgr.cancel();
        }
    }
    Ok(())
}
