use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream, sql::TableReference,
};
use snafu::prelude::*;

#[cfg(feature = "duckdb")]
pub mod duckdbconn;
#[cfg(feature = "mysql")]
pub mod mysqlconn;
#[cfg(feature = "odbc")]
pub mod odbcconn;
#[cfg(feature = "postgres")]
pub mod postgresconn;
#[cfg(feature = "sqlite")]
pub mod sqliteconn;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to downcast connection.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    UnableToDowncastConnection {},

    #[snafu(display("{source}"))]
    UnableToGetSchema { source: GenericError },

    #[snafu(display("The field '{field_name}' has an unsupported data type: {data_type}."))]
    UnsupportedDataType {
        data_type: String,
        field_name: String,
    },

    #[snafu(display("Failed to execute query.\n{source}"))]
    UnableToQueryArrow { source: GenericError },

    #[snafu(display(
        "Table '{table_name}' not found. Ensure the table name is correctly spelled."
    ))]
    UndefinedTable {
        table_name: String,
        source: GenericError,
    },

    #[snafu(display("Unable to get schemas: {source}"))]
    UnableToGetSchemas { source: GenericError },

    #[snafu(display("Unable to get tables: {source}"))]
    UnableToGetTables { source: GenericError },
}

#[async_trait::async_trait]
pub trait DbConnection: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    async fn tables(&self, schema: &str) -> Result<Vec<String>, Error>;

    async fn schemas(&self) -> Result<Vec<String>, Error>;

    /// Get the schema for a table reference.
    ///
    /// # Arguments
    ///
    /// * `table_reference` - The table reference.
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error>;

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
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream>;

    /// Execute the given SQL statement with parameters, returning the number of affected rows.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    async fn execute(&self, sql: &str) -> Result<u64>;
}

pub async fn get_tables(conn: impl DbConnection, schema: &str) -> Result<Vec<String>, Error> {
    conn.tables(schema).await
}

/// Get the schemas for the database.
///
/// # Errors
///
/// Returns an error if the schemas cannot be retrieved.
pub async fn get_schemas(conn: impl DbConnection) -> Result<Vec<String>, Error> {
    conn.schemas().await
}

/// Get the schema for a table reference.
///
/// # Arguments
///
/// * `conn` - The database connection.
/// * `table_reference` - The table reference.
///
/// # Errors
///
/// Returns an error if the schema cannot be retrieved.
pub async fn get_schema(
    conn: impl DbConnection,
    table_reference: &datafusion::sql::TableReference,
) -> Result<Arc<datafusion::arrow::datatypes::Schema>, Error> {
    conn.get_schema(table_reference).await
}

/// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
///
/// # Arguments
///
/// * `conn` - The database connection.
/// * `sql` - The SQL statement.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn query_arrow(
    conn: impl DbConnection,
    sql: String,
    projected_schema: Option<SchemaRef>,
) -> Result<SendableRecordBatchStream, Error> {
    conn.query_arrow(&sql, projected_schema)
        .await
        .context(UnableToQueryArrowSnafu {})
}
