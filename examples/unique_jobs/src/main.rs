use std::time::Duration;

use jiff::ToSpan;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use underway::{task::UniqueJobStrategy, Queue, Task, Worker};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EnsureUserSync {
    user_id: String,
    metadata: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RenderReport {
    report_id: String,
    content_hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CreateInvoice {
    invoice_id: String,
    amount: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum AppJob {
    SyncUserMetadata(EnsureUserSync),
    GenerateReport(RenderReport),
    ProcessInvoice(CreateInvoice),
}

struct ExampleTask;

impl Task for ExampleTask {
    type Input = AppJob;
    type Output = ();

    fn concurrency_key_for(&self, input: &Self::Input) -> Option<String> {
        match input {
            AppJob::SyncUserMetadata(sync) => Some(format!("sync-user-{}", sync.user_id)),
            AppJob::GenerateReport(report) => Some(format!("render-report-{}", report.report_id)),
            AppJob::ProcessInvoice(invoice) => {
                Some(format!("process-invoice-{}", invoice.invoice_id))
            }
        }
    }

    fn unique_strategy_for(&self, input: &Self::Input) -> UniqueJobStrategy {
        match input {
            // Do nothing if the existing sync job is already pending, ignoring the new one
            AppJob::SyncUserMetadata(_) => UniqueJobStrategy::DoNothing,
            // Replace the existing pending report generation with the newest request
            AppJob::GenerateReport(_) => UniqueJobStrategy::Replace,
            // Error out if a duplicate invoice processing job is enqueued
            AppJob::ProcessInvoice(_) => UniqueJobStrategy::Strict,
        }
    }

    async fn execute(
        &self,
        _tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> underway::task::Result<Self::Output> {
        match input {
            AppJob::SyncUserMetadata(sync) => {
                println!(
                    "Syncing user metadata for user {}: {}",
                    sync.user_id, sync.metadata
                );
            }
            AppJob::GenerateReport(report) => {
                println!(
                    "Generating report {} (hash: {})",
                    report.report_id, report.content_hash
                );
            }
            AppJob::ProcessInvoice(invoice) => {
                println!(
                    "Processing invoice {}: ${}",
                    invoice.invoice_id, invoice.amount
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    underway::run_migrations(&pool).await?;

    let queue: Queue<ExampleTask> = Queue::builder()
        .name("unique-jobs-example")
        .pool(pool.clone())
        .build()
        .await?;

    // Start running tasks in the background
    let worker = Worker::new(queue.clone().into(), ExampleTask);

    tokio::spawn({
        let worker = worker.clone();
        async move { worker.run().await }
    });

    // 1. Strict Strategy
    println!("--- Strict Strategy ---");
    println!("Enqueueing initial invoice processing for invoice-1");

    queue
        .enqueue(
            &pool,
            &ExampleTask,
            &AppJob::ProcessInvoice(CreateInvoice {
                invoice_id: "1".to_string(),
                amount: 100,
            }),
        )
        .await?;

    println!("Attempting to enqueue a conflicting invoice for invoice-1");
    // This will return an error because the strategy is Strict
    let result = queue
        .enqueue(
            &pool,
            &ExampleTask,
            &AppJob::ProcessInvoice(CreateInvoice {
                invoice_id: "1".to_string(),
                amount: 100,
            }),
        )
        .await;

    match result {
        Ok(_) => println!("Unexpectedly enqueued a strict duplicate job!"),
        Err(e) => println!("Correctly received an error on strict conflict: {}", e),
    }

    // Wait for the first job to finish processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 2. DoNothing Strategy
    println!("\n--- DoNothing Strategy ---");
    println!("Enqueueing initial sync for user-123");

    queue
        .enqueue(
            &pool,
            &ExampleTask,
            &AppJob::SyncUserMetadata(EnsureUserSync {
                user_id: "123".to_string(),
                metadata: "initial_data".to_string(),
            }),
        )
        .await?;

    println!("Attempting to enqueue a conflicting sync for user-123");
    // This job should be ignored because one is already in the queue or in progress
    queue
        .enqueue(
            &pool,
            &ExampleTask,
            &AppJob::SyncUserMetadata(EnsureUserSync {
                user_id: "123".to_string(),
                metadata: "ignored_updates".to_string(),
            }),
        )
        .await?;

    // Wait for the first job to finish processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Replace Strategy
    println!("\n--- Replace Strategy ---");
    println!("Enqueueing first report generation for report-1");

    // We'll enqueue it with a delay so it stays pending while we enqueue the second
    // one
    queue
        .enqueue_after(
            &pool,
            &ExampleTask,
            &AppJob::GenerateReport(RenderReport {
                report_id: "1".to_string(),
                content_hash: "v1".to_string(),
            }),
            2_i64.seconds(),
        )
        .await?;

    println!("Enqueueing newer report generation for report-1, displacing the first");
    // This will overwrite the inputs of the pending job
    queue
        .enqueue_after(
            &pool,
            &ExampleTask,
            &AppJob::GenerateReport(RenderReport {
                report_id: "1".to_string(),
                content_hash: "v2-updated".to_string(),
            }),
            2_i64.seconds(),
        )
        .await?;

    // Wait for the delayed job to finish
    tokio::time::sleep(Duration::from_secs(4)).await;

    worker.shutdown();

    Ok(())
}
