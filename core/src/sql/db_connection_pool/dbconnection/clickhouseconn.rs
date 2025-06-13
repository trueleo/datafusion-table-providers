use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_ipc::reader::StreamDecoder;
use async_trait::async_trait;
use clickhouse::{Client, Row};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::sql::arrow_sql_gen::clickhouse::clickhouse_type_to_arrow_type;

use super::{AsyncDbConnection, DbConnection, Error, SyncDbConnection};

impl<P: Serialize + Sync> DbConnection<Client, P> for Client {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(&self) -> Option<&dyn SyncDbConnection<Client, P>> {
        None
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Client, P>> {
        Some(self)
    }
}

#[async_trait]
impl<P: Serialize + Sync> AsyncDbConnection<Client, P> for Client {
    fn new(conn: Client) -> Self
    where
        Self: Sized,
    {
        conn
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, Error> {
        #[derive(Row, Deserialize)]
        struct Row {
            name: String,
        }

        let tables: Vec<Row> = self
            .query("SHOW TABLES FROM ?")
            .bind(schema)
            .fetch_all()
            .await
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;

        Ok(tables.into_iter().map(|x| x.name).collect())
    }

    async fn schemas(&self) -> Result<Vec<String>, Error> {
        #[derive(Row, Deserialize)]
        struct Row {
            name: String,
        }
        let tables: Vec<Row> = self
            .query("SHOW DATABASES")
            .fetch_all()
            .await
            .boxed()
            .context(super::UnableToGetSchemasSnafu)?;

        Ok(tables.into_iter().map(|x| x.name).collect())
    }

    /// Get the schema for a table reference.
    ///
    /// # Arguments
    ///
    /// * `table_reference` - The table reference.
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error> {
        #[derive(Row, Deserialize)]
        struct CatalogRow {
            db: String,
        }

        let database = match table_reference.schema() {
            Some(db) => db.to_string(),
            None => {
                let row: CatalogRow = self
                    .query("SELECT currentDatabase() AS db")
                    .fetch_one()
                    .await
                    .boxed()
                    .context(super::UnableToGetSchemaSnafu)?;
                row.db
            }
        };

        #[derive(Row, Deserialize)]
        struct Row {
            column_name: String,
            data_type: String,
        }

        let table = table_reference.table();

        let tables: Vec<Row> = self
            .query("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?")
            .bind(&database) // table_schema
            .bind(table) // table_name
            .fetch_all()
            .await
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let fields: Result<Vec<_>, Error> = tables
            .iter()
            .map(|x| {
                clickhouse_type_to_arrow_type(&x.data_type)
                    .map(|dt| Field::new(x.column_name.clone(), dt, true))
                    .ok_or_else(|| Error::UnsupportedDataType {
                        data_type: x.data_type.clone(),
                        field_name: x.column_name.clone(),
                    })
            })
            .collect();

        Ok(Arc::new(Schema::new(fields?)))
    }

    /// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    /// * `projected_schema` - The Projected schema for the query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[P],
        projected_schema: Option<SchemaRef>,
    ) -> super::Result<SendableRecordBatchStream> {
        let mut query = self.query(sql);
        for param in params {
            query = query.bind(param);
        }

        let mut bytes_stream = query
            .fetch_bytes("ArrowStream")
            .boxed()
            .context(super::UnableToQueryArrowSnafu)?;

        let mut first_batch: Option<RecordBatch> = None;
        let mut decoder = StreamDecoder::new();

        // fetch till first set of records
        while let Some(buf) = bytes_stream.next().await? {
            if let Some(batch) = decoder.decode(&mut buf.into())? {
                first_batch = Some(batch);
                break;
            }
        }

        if let Some(first_batch) = first_batch {
            let schema = first_batch.schema();
            let stream = async_stream::stream! {
                while let Some(buf) = bytes_stream
                    .next()
                    .await
                    .map_err(|er| arrow::error::ArrowError::ExternalError(Box::new(er)))?
                {
                    if let Some(batch) = decoder.decode(&mut buf.into())? {
                        yield Ok(batch);
                    }
                }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        } else if let Some(schema) = projected_schema {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                EmptyRecordBatchStream::new(schema),
            )))
        } else {
            let schema: Arc<Schema> = Schema::empty().into();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                EmptyRecordBatchStream::new(schema),
            )))
        }
    }

    /// Execute the given SQL statement with parameters, returning the number of affected rows.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    async fn execute(&self, sql: &str, params: &[P]) -> super::Result<u64> {
        let mut query = self.query(sql);

        for param in params {
            query = query.bind(param);
        }

        query
            .execute()
            .await
            .boxed()
            .context(super::UnableToQueryArrowSnafu)?;

        Ok(0)
    }
}
