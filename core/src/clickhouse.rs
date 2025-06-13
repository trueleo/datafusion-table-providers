/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::sql::db_connection_pool::dbconnection::{AsyncDbConnection, DbConnection};
use crate::sql::db_connection_pool::{self, DbConnectionPool};
use crate::sql::sql_provider_datafusion;
// use crate::util::{
//     self, column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
//     indexes::IndexType, on_conflict::OnConflict, secrets::to_secret_map, to_datafusion_error,
// };
use crate::util::{column_reference, constraints, on_conflict};
// use async_trait::async_trait;
use clickhouse::{Client, Row};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::sql::unparser;
// use datafusion::catalog::Session;
// use datafusion::{
//     catalog::TableProviderFactory, datasource::TableProvider,
//     error::DataFusionError, logical_expr::CreateExternalTable, sql::TableReference,
// };
use datafusion::{common::Constraints, sql::TableReference};
use serde::Deserialize;
use snafu::prelude::*;
use std::sync::Arc;

pub type DynClickhouseConnectionPool =
    dyn DbConnectionPool<clickhouse::Client, &'static (dyn serde::Serialize + Sync)> + Send + Sync;

pub type DynClickhouseConnection =
    dyn DbConnection<clickhouse::Client, &'static (dyn serde::Serialize + Sync)>;

#[cfg(feature = "clickhouse-federation")]
pub mod federation;
// pub(crate) mod mysql_window;
pub mod sql_table;
// pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to delete all data from the MySQL table: {source}"))]
    UnableToDeleteAllTableData { source: clickhouse::error::Error },

    #[snafu(display("Unable to insert Arrow batch to MySQL table: {source}"))]
    UnableToInsertArrowBatch { source: clickhouse::error::Error },

    #[snafu(display("Unable to downcast DbConnection to MySQLConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to begin MySQL transaction: {source}"))]
    UnableToBeginTransaction { source: clickhouse::error::Error },

    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: clickhouse::error::Error },

    #[snafu(display("Unable to create the MySQL table: {source}"))]
    UnableToCreateClickHouseTable { source: clickhouse::error::Error },

    #[snafu(display("Unable to create an index for the MySQL table: {source}"))]
    UnableToCreateIndexForClickHouseTable { source: clickhouse::error::Error },

    #[snafu(display("Unable to commit the MySQL transaction: {source}"))]
    UnableToCommitMySQLTransaction { source: clickhouse::error::Error },

    #[snafu(display("Unable to create insertion statement for MySQL table: {source}"))]
    UnableToCreateInsertStatement {
        source: crate::sql::arrow_sql_gen::statement::Error,
    },

    #[snafu(display("The table '{table_name}' doesn't exist in the MySQL server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ClickHouseTableFactory {
    client: clickhouse::Client,
}

impl ClickHouseTableFactory {
    pub fn new(pool: clickhouse::Client) -> Self {
        Self { client: pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let client: &dyn AsyncDbConnection<Client, ()> = &self.client;
        let schema = client.get_schema(&table_reference).await?;
        let table_provider = Arc::new(ClickHouseTable::new(
            table_reference,
            self.client.clone(),
            schema,
            Constraints::empty(),
        ));

        #[cfg(feature = "clickhouse-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }

    // pub async fn read_write_table_provider(
    //     &self,
    //     table_reference: TableReference,
    // ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
    //     let read_provider = Self::table_provider(self, table_reference.clone()).await?;
    //     let schema = read_provider.schema();

    //     let table_name = table_reference.to_string();
    //     let mysql = MySQL::new(
    //         table_name,
    //         Arc::clone(&self.pool),
    //         schema,
    //         Constraints::empty(),
    //     );

    //     Ok(ClickHouseTableWriter::create(read_provider, mysql, None))
    // }
}

#[derive(Debug)]
pub struct ClickHouseTableProviderFactory {}

impl ClickHouseTableProviderFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ClickHouseTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

// #[async_trait]
// impl TableProviderFactory for ClickHouseTableProviderFactory {
//     async fn create(
//         &self,
//         _state: &dyn Session,
//         cmd: &CreateExternalTable,
//     ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
//         let name = cmd.name.to_string();
//         let mut options = cmd.options.clone();
//         let schema: Schema = cmd.schema.as_ref().into();

//         let indexes_option_str = options.remove("indexes");
//         let unparsed_indexes: HashMap<String, IndexType> = match indexes_option_str {
//             Some(indexes_str) => util::hashmap_from_option_string(&indexes_str),
//             None => HashMap::new(),
//         };

//         let params = to_secret_map(options);

//         // todo: initiate client with params
//         let client = Client::default();
//         let schema = Arc::new(schema);

//         let clickhouse_table = ClickHouseTable::new(
//             TableReference::from(name),
//             client,
//             schema,
//             cmd.constraints.clone(),
//         );

//         let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

//         clickhouse_table
//             .create_table(Arc::clone(&schema), primary_keys)
//             .await
//             .map_err(to_datafusion_error)?;

//         let read_provider = Arc::new(clickhouse_table);

//         #[cfg(feature = "clickhouse-federation")]
//         let read_provider = Arc::new(read_provider.create_federated_table_provider()?);

//         Ok(ClickHouseTableWriter::create(
//             read_provider,
//             mysql,
//             on_conflict,
//         ))
//     }
// }

pub struct ClickHouseTable {
    table_reference: TableReference,
    pool: Client,
    schema: SchemaRef,
    constraints: Constraints,
    dialect: Arc<dyn unparser::dialect::Dialect>,
}

impl std::fmt::Debug for ClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("table_name", &self.table_reference)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl ClickHouseTable {
    pub fn new(
        table_reference: TableReference,
        pool: Client,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_reference,
            pool,
            schema,
            constraints,
            dialect: Arc::new(unparser::dialect::DefaultDialect {}),
        }
    }

    pub fn table_name(&self) -> &TableReference {
        &self.table_reference
    }

    pub fn client(&self) -> &Client {
        &self.pool
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub async fn connect(&self) -> Result<Client> {
        let conn = self.pool.clone();
        if !self.table_exists().await? {
            TableDoesntExistSnafu {
                table_name: self.table_reference.to_string(),
            }
            .fail()?;
        }
        Ok(conn)
    }

    pub async fn table_exists(&self) -> Result<bool> {
        let sql = r#"SELECT EXISTS (
          SELECT 1 as exist
          FROM information_schema.tables
          WHERE table_name = ?
        )"#;

        tracing::trace!("{sql}");

        #[derive(Row, Deserialize)]
        struct Row {
            #[allow(dead_code, reason = "this is only used to deserialize")]
            exist: u8,
        }

        let row = self
            .client()
            .query(&sql)
            .bind(self.table_reference.table())
            .fetch_optional::<Row>()
            .await
            .boxed()
            .context(DbConnectionSnafu)?;

        Ok(row.is_some())
    }

    // async fn insert_batch(
    //     &self,
    //     transaction: &mut mysql_async::Transaction<'_>,
    //     batch: RecordBatch,
    //     on_conflict: Option<OnConflict>,
    // ) -> Result<()> {
    //     todo!()
    // }

    // async fn delete_all_table_data(
    //     &self,
    //     transaction: &mut mysql_async::Transaction<'_>,
    // ) -> Result<()> {
    //     todo!()
    // }

    // async fn create_table(&self, schema: SchemaRef, primary_keys: Vec<String>) -> Result<()> {
    //     todo!()
    // }

    // async fn create_index(
    //     &self,
    //     transaction: &mut mysql_async::Transaction<'_>,
    //     columns: Vec<&str>,
    //     unique: bool,
    // ) -> Result<()> {
    //     let mut index_builder = IndexBuilder::new(&self.table_name, columns);
    //     todo!()
    // }
}
