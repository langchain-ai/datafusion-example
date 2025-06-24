use arrow_array::{BinaryViewArray, StringArray};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("DataFusion Query Runner");
    println!("=======================\n");

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data/datalake-single-large-file_optimized_slim_chatlangchain-lg-json.parquet");
    let file_path = path.to_str().unwrap();

    println!("Reading parquet file from: {}", file_path);

    let config = SessionConfig::new()
        .set(
            "datafusion.execution.parquet.pruning",
            &ScalarValue::Boolean(Some(true)),
        )
        .set(
            "datafusion.execution.parquet.enable_page_index",
            &ScalarValue::Boolean(Some(true)),
        )
        .set(
            "datafusion.execution.parquet.pushdown_filters",
            &ScalarValue::Boolean(Some(true)),
        )
        .set(
            "datafusion.execution.parquet.reorder_filters",
            &ScalarValue::Boolean(Some(true)),
        );

    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet("runs", file_path, ParquetReadOptions::default())
        .await?;

    let sql = r#"
SELECT
    json_payload
FROM runs
WHERE id = '2ef7079b-541a-4229-bd00-e6c00402e8f1'
"#;

    println!("Query:");
    println!("{}", sql);
    println!("\n{}\n", "=".repeat(80));

    println!("EXPLAIN ANALYZE Output:");
    println!("{}", "-".repeat(80));

    let explain_sql = format!("EXPLAIN ANALYZE {}", sql);
    let explain_df = ctx.sql(&explain_sql).await?;
    let explain_results = explain_df.collect().await?;

    for batch in &explain_results {
        let plan_type = batch.column(0);
        let plan = batch.column(1);

        for i in 0..batch.num_rows() {
            let plan_type_str = plan_type
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i);
            let plan_str = plan
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i);

            println!("{}: {}", plan_type_str, plan_str);
        }
    }

    println!("\n{}\n", "=".repeat(80));

    println!("Running Actual Query...");
    let start = Instant::now();
    let df = ctx.sql(sql).await?;

    let logical_plan = df.logical_plan().clone();
    let results = df.collect().await?;
    let elapsed = start.elapsed();

    println!(
        "Query executed in: {:.3} ms",
        elapsed.as_secs_f64() * 1000.0
    );

    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!("Total rows returned: {}", total_rows);

    if total_rows > 0 {
        println!("\nFirst 5 rows (json_payload truncated to 100 chars):");
        println!("{}", "-".repeat(80));

        let mut row_count = 0;
        for batch in &results {
            if row_count >= 5 {
                break;
            }

            let json_payload = batch.column(0);
            let json_array = json_payload
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap();

            for i in 0..batch.num_rows().min(5 - row_count) {
                let value = json_array.value(i);
                let value_str = String::from_utf8_lossy(value);
                let truncated = if value_str.len() > 100 {
                    format!("{}...", &value_str[..100])
                } else {
                    value_str.to_string()
                };
                println!("Row {}: {}", row_count + 1, truncated);
                row_count += 1;
            }
        }
    }

    println!("\n{}\n", "=".repeat(80));

    println!("Logical Plan:");
    println!("{}", logical_plan.display_indent());

    println!("\nOptimized Logical Plan:");
    let state = ctx.state();
    let optimized_plan = state.optimize(&logical_plan)?;
    println!("{}", optimized_plan.display_indent());

    println!("\nPhysical Plan:");
    let physical_plan = state.create_physical_plan(&optimized_plan).await?;
    println!(
        "{}",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(false)
    );

    Ok(())
}
