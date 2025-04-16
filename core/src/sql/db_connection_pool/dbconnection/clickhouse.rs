use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use async_stream::stream;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, Stream, StreamExt};
use klickhouse::block::Block;
use klickhouse::Client;
use snafu::prelude::*;

use crate::sql::arrow_sql_gen::clickhouse::block_to_arrow;
use crate::sql::db_connection_pool::dbconnection::Error;
use crate::sql::db_connection_pool::dbconnection::{self, AsyncDbConnection, DbConnection};

use super::UnableToQueryArrowSnafu;

pub struct ClickhouseConnection {
    pub conn: Client,
}

impl ClickhouseConnection {
    pub fn new(conn: Client) -> Self {
        Self { conn }
    }
}

impl<'a> DbConnection<Client, &'a (dyn Sync)> for ClickhouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Client, &'a (dyn Sync)>> {
        Some(self)
    }
}

// Clickhouse doesn't have a params in signature. So just setting to `dyn Sync`.
// Looks like we don't actually pass any params to query_arrow.
// But keep it in mind.
#[async_trait::async_trait]
impl<'a> AsyncDbConnection<Client, &'a (dyn Sync)> for ClickhouseConnection {
    // Required by trait, but not used.
    fn new(_: Client) -> Self {
        unreachable!()
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, Error> {
        unimplemented!()
    }

    async fn schemas(&self) -> Result<Vec<String>, Error> {
        unimplemented!()
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, dbconnection::Error> {
        unimplemented!()
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _: &[&'a (dyn Sync)],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, Box<dyn std::error::Error + Send + Sync>> {
        let mut block_stream = self
            .conn
            .query_raw(sql.to_string())
            .await
            .boxed()
            .context(UnableToQueryArrowSnafu)?;

        let first_block = block_stream.next().await;

        let Some(first_block) = first_block else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                stream::empty(),
            )));
        };

        let first_block = first_block.boxed().context(UnableToQueryArrowSnafu)?;
        let rec = block_to_arrow(first_block)
            .boxed()
            .context(UnableToQueryArrowSnafu)?;
        let schema = rec.schema();
        let stream_adapter =
            RecordBatchStreamAdapter::new(schema, query_to_stream(rec, Box::pin(block_stream)));

        Ok(Box::pin(stream_adapter))
    }

    async fn execute(
        &self,
        query: &str,
        _: &[&'a (dyn Sync)],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.conn.execute(query).await.boxed()?;
        Ok(0)
    }
}

fn query_to_stream(
    first_batch: RecordBatch,
    mut block_stream: Pin<
        Box<dyn Stream<Item = Result<Block, klickhouse::KlickhouseError>> + Send>,
    >,
) -> impl Stream<Item = datafusion::common::Result<RecordBatch>> {
    stream! {
       yield Ok(first_batch);
       while let Some(block) = block_stream.next().await {
            match block {
                Ok(block) => {
                    let rec = block_to_arrow(block);
                    match rec {
                        Ok(rec) => {
                            yield Ok(rec);
                        }
                        Err(e) => {
                            yield Err(DataFusionError::Execution(format!("Failed to convert query result to Arrow: {e}")));
                        }
                    }
                }
                Err(e) => {
                    yield Err(DataFusionError::Execution(format!("Failed to fetch block: {e}")));
                }
            }
       }
    }
}
