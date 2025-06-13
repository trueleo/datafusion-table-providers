use async_trait::async_trait;
use clickhouse::Client;
use datafusion::catalog::Session;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::sql::unparser::Unparser;
use futures::TryStreamExt;
use std::fmt::Display;
use std::{any::Any, fmt, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, SendableRecordBatchStream,
    },
};

use crate::sql::db_connection_pool::dbconnection::query_arrow;
use crate::sql::sql_provider_datafusion::{
    default_supported_filters, project_schema_safe, to_execution_error,
};

use super::ClickHouseTable;

impl ClickHouseTable {
    fn create_logical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<LogicalPlan> {
        let table_source = LogicalTableSource::new(self.schema().clone());
        LogicalPlanBuilder::scan_with_filters(
            self.table_reference.clone(),
            Arc::new(table_source),
            projection.cloned(),
            filters.to_vec(),
        )?
        .limit(0, limit)?
        .build()
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.create_logical_plan(projections, filters, limit)?;
        let sql = Unparser::new(&*self.dialect)
            .plan_to_sql(&logical_plan)?
            .to_string();
        Ok(Arc::new(ClickHouseExec::new(
            projections,
            schema,
            self.pool.clone(),
            sql,
        )?))
    }
}

#[async_trait]
impl TableProvider for ClickHouseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let filter_push_down = default_supported_filters(filters, &*self.dialect);
        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

impl Display for ClickHouseTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseTable {}", self.table_name())
    }
}

struct ClickHouseExec {
    projected_schema: SchemaRef,
    pool: Client,
    sql: String,
    properties: PlanProperties,
}

impl fmt::Debug for ClickHouseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseExec")
            .field("projected_schema", &self.projected_schema)
            .field("sql", &self.sql)
            .field("properties", &self.properties)
            .finish()
    }
}

impl ClickHouseExec {
    pub fn new(
        projection: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Client,
        sql: String,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema_safe(schema, projection)?;

        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            pool,
            sql,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }
}

impl DisplayAs for ClickHouseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = &self.sql;
        write!(f, "ClickHouseExec sql={sql}")
    }
}

impl ExecutionPlan for ClickHouseExec {
    fn name(&self) -> &'static str {
        "ClickHouseExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = &self.sql;
        tracing::debug!("SqlExec sql: {sql}");
        let schema = self.schema();
        let fut = {
            let conn = self.pool.clone();
            let sql = sql.clone();
            let projected_schema = Arc::clone(&schema);
            async move {
                query_arrow::<Client, ()>(conn, sql, Some(projected_schema))
                    .await
                    .map_err(to_execution_error)
            }
        };
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
