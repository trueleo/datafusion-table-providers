use std::sync::Arc;

use crate::sql::db_connection_pool::{dbconnection::DbConnection, DbConnectionPool, JoinPushDown};
use async_trait::async_trait;
use klickhouse::bb8::ManageConnection;
use klickhouse::{Client, ClientOptions, ConnectionManager};

use super::dbconnection::clickhouse::ClickhouseConnection;

pub struct ClickhouseConnectionPool {
    pool: Arc<klickhouse::ConnectionManager>,
    join_push_down: JoinPushDown,
}

impl ClickhouseConnectionPool {
    // Creates a new instance of `ClickhouseConnectionPool`.
    pub async fn new<A: tokio::net::ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        compute_context: String,
    ) -> Result<Self, std::io::Error> {
        let pool = ConnectionManager::new(destination, options).await?;
        Ok(Self {
            pool: Arc::new(pool),
            join_push_down: JoinPushDown::AllowedFor(compute_context),
        })
    }
}

#[async_trait]
impl DbConnectionPool<Client, &'static (dyn Sync)> for ClickhouseConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<dyn DbConnection<Client, &'static (dyn Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool.connect().await.map_err(Box::new)?;
        Ok(Box::new(ClickhouseConnection::new(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}
