//! seql-substrait — SeQL to Substrait compiler and plan reducer

pub mod seql_ext {
    include!(concat!(env!("OUT_DIR"), "/seql_extension.rs"));
}
mod compiler;
pub mod reducer;

pub use compiler::{
    apply_aggregate, apply_compute, apply_filter, apply_limit, apply_project, apply_sort,
    apply_unique, ast_expr_to_df_expr, ast_to_logical_plan, compile, compile_ast, decode_plan_ext,
    decode_plan_meta, raw_scan_plan, schema_context, set_plan_mode, set_plan_scope,
    signal_table_name, time_column_for_signal, PlanMeta,
};
pub use reducer::*;
