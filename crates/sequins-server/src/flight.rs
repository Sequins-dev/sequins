//! Arrow Flight SQL server for Sequins
//!
//! Implements `FlightSqlService` accepting Substrait plans via
//! `CommandStatementSubstraitPlan` and returning Arrow IPC `FlightData` streams.
//!
//! # Protocol flow
//!
//! 1. Client sends `GetFlightInfo(CommandStatementSubstraitPlan{plan_bytes})`.
//! 2. Server responds with `FlightInfo` containing a single endpoint whose
//!    `Ticket` carries the raw plan bytes.
//! 3. Client calls `DoGet(Ticket{...})`.
//! 4. Server executes the plan via `QueryExec::execute()` and streams `FlightData`.

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionEndSavepointRequest, ActionEndTransactionRequest,
    CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, DoPutPreparedStatementResult, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use futures::StreamExt;
use sequins_query::QueryExec;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

/// Shared state for the Flight SQL server
#[derive(Clone)]
pub struct FlightSqlState {
    pub query_exec: Arc<dyn QueryExec>,
}

/// Arrow Flight SQL service implementation
pub struct SequinsFlightSqlService {
    state: FlightSqlState,
}

impl SequinsFlightSqlService {
    pub fn new(query_exec: Arc<dyn QueryExec>) -> Self {
        Self {
            state: FlightSqlState { query_exec },
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for SequinsFlightSqlService {
    type FlightService = Self;

    // ── Handshake ──────────────────────────────────────────────────────────

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<
            std::pin::Pin<
                Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>,
            >,
        >,
        Status,
    > {
        // No authentication required
        let resp = futures::stream::empty();
        Ok(Response::new(Box::pin(resp)))
    }

    // ── GetFlightInfo for Substrait plan ───────────────────────────────────

    /// Return a `FlightInfo` for a Substrait plan.
    ///
    /// The plan bytes are embedded directly in the `Ticket` so the client
    /// can immediately call `DoGet` — no server-side state needed.
    async fn get_flight_info_substrait_plan(
        &self,
        cmd: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let plan_bytes = cmd
            .plan
            .as_ref()
            .map(|p| p.plan.clone())
            .unwrap_or_default();

        let ticket = Ticket { ticket: plan_bytes };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            ..Default::default()
        };
        let info = FlightInfo {
            endpoint: vec![endpoint],
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    // ── DoGet fallback — executes the plan from the Ticket ─────────────────

    /// Execute a Substrait plan carried in the `Ticket`.
    ///
    /// The raw bytes from the `Ticket` are passed directly to
    /// `QueryExec::execute()` which decodes and runs the plan.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: arrow_flight::sql::Any,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        let plan_bytes = request.into_inner().ticket.to_vec();
        let stream = self
            .state
            .query_exec
            .execute(plan_bytes)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Map QueryError → Status
        let mapped = stream.map(|r| r.map_err(|e| Status::internal(e.to_string())));
        Ok(Response::new(Box::pin(mapped)))
    }

    // ── All other FlightSql operations are not supported ───────────────────

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "SQL text queries not supported; use Substrait",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Prepared statements not supported"))
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_catalogs not supported",
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_schemas not supported",
        ))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_tables not supported",
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_table_types not supported",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_sql_info not supported",
        ))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_primary_keys not supported",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_exported_keys not supported",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not supported",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_cross_reference not supported",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_xdbc_type_info not supported",
        ))
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_statement not supported"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented(
            "do_get_prepared_statement not supported",
        ))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_catalogs not supported"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_schemas not supported"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_tables not supported"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_table_types not supported"))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_sql_info not supported"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_primary_keys not supported"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_exported_keys not supported"))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_imported_keys not supported"))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented(
            "do_get_cross_reference not supported",
        ))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<
        Response<std::pin::Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_get_xdbc_type_info not supported"))
    }

    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_update not supported",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query not supported",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_update not supported",
        ))
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("do_put_substrait_plan not supported"))
    }

    async fn do_put_statement_ingest(
        &self,
        _ticket: CommandStatementIngest,
        _request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_ingest not supported",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "create_prepared_statement not supported",
        ))
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "close_prepared_statement not supported",
        ))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("begin_savepoint not supported"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("end_savepoint not supported"))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented("begin_transaction not supported"))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("end_transaction not supported"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("cancel_query not supported"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Build the gRPC `FlightServiceServer` for use with `tonic::transport::Server`.
///
/// `FlightSqlService` implementors automatically get a blanket `FlightService` impl,
/// so wrapping in `FlightServiceServer::new()` is all that's needed.
pub fn flight_service_server(
    query_exec: Arc<dyn QueryExec>,
) -> arrow_flight::flight_service_server::FlightServiceServer<SequinsFlightSqlService> {
    arrow_flight::flight_service_server::FlightServiceServer::new(SequinsFlightSqlService::new(
        query_exec,
    ))
}
