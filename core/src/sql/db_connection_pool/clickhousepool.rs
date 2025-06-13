use clickhouse::Client;
use serde::Serialize;

use super::{dbconnection::DbConnection, DbConnectionPool, JoinPushDown};

#[async_trait::async_trait]
impl<P: Serialize + Sync + 'static> DbConnectionPool<Client, P> for Client {
    async fn connect(&self) -> super::Result<Box<dyn DbConnection<Client, P>>> {
        Ok(Box::new(self.clone()))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::Disallow
    }
}
