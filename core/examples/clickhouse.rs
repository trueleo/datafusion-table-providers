use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::{
    clickhouse::ClickHouseTableFactory, common::DatabaseCatalogProvider,
    util::secrets::to_secret_map,
};
use std::collections::HashMap;
use std::sync::Arc;

/// This example demonstrates how to:
/// 1. Create a PostgreSQL connection pool
/// 2. Create and use PostgresTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access PostgreSQL table data
///
/// Prerequisites:
/// Start a PostgreSQL server using Docker:
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create a table and insert sample data
/// docker exec -i postgres psql -U postgres test_db <<EOF
/// CREATE TABLE companies (
///    id SERIAL PRIMARY KEY,
///    name VARCHAR(100)
/// );
///
/// INSERT INTO companies (name) VALUES ('Example Corp');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    // Create PostgreSQL connection parameters
    let client = clickhouse::Client::default()
        .with_url("http://localhost:8123")
        .with_user("admin")
        .with_password("secret");

    // Create PostgreSQL table provider factory
    // Used to generate TableProvider instances that can read PostgreSQL table data
    let table_factory = ClickHouseTableFactory::new(client);
    let table = table_factory.table_provider(TableReference::bare("SubscribersData"));

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Demonstrate direct table provider registration
    ctx.register_table("users", table.await.unwrap())
        .expect("failed to register table");

    // Query Example 1: Query the renamed table through default catalog
    let df = ctx
        .sql("SELECT * FROM datafusion.public.users")
        .await
        .expect("select failed");

    df.show().await.expect("show failed");
}
