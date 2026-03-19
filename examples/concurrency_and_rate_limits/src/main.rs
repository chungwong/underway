use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use underway::{Queue, Task, Worker};

// ==============================================================================
//  1. HEAVY CPU WORK: Local Concurrency Example (max_concurrency)
// ==============================================================================
// Best for: Ensuring your local server/DB isn't crushed by 10,000 parallel
// jobs. It simply limits how many workers can check out tasks.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RunExpensiveJob {
    job_id: usize,
}

struct HeavyWorkTask;

impl Task for HeavyWorkTask {
    type Input = RunExpensiveJob;
    type Output = ();

    async fn execute(
        &self,
        _tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> underway::task::Result<Self::Output> {
        println!(
            "🏋️ [CPU Task] Job {} dequeued! Processing heavy local work...",
            input.job_id
        );

        // Simulate a slow API request or heavy computation
        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("✅ [CPU Task Finished] Job {} completed!", input.job_id);
        Ok(())
    }
}

// ==============================================================================
//  2. EXTERNAL SAAS API: Global Rate Limiting Example (global_rate_limit)
// ==============================================================================
// Best for: Enforcing tight 3rd party API limits (e.g. 2 requests / 10
// seconds). Coordinates natively across EVERY running worker instantly,
// preventing 429s. It delays execution gracefully into the future with zero
// "busy looping".
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ScrapeUrlJob {
    url: String,
}

struct WebScraperTask;

impl Task for WebScraperTask {
    type Input = ScrapeUrlJob;
    type Output = ();

    async fn execute(
        &self,
        _tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> underway::task::Result<Self::Output> {
        println!(
            "🌐 [API Rate Limiter Passed] ScrapeUrlJob for {} gracefully dequeued! Executing...",
            input.url
        );

        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }
}

// ==============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    underway::run_migrations(&pool).await?;

    // Safety check just for the demo: clear the rate limiter bucket automatically
    underway::rate_limit::clear(&pool, "scraper_api_test").await?;

    println!("--- CONFIGURING LOCAL CONCURRENCY QUEUE ---");
    let heavy_queue: Queue<HeavyWorkTask> = Queue::builder()
        .name("heavy-cpu-tasks-queue")
        .max_concurrency(2)
        .pool(pool.clone())
        .build()
        .await?;

    println!("--- CONFIGURING GLOBAL RATE LIMIT WEB API QUEUE ---");
    let web_queue: Queue<WebScraperTask> = Queue::builder()
        .name("web-scraper-api-queue")
        .global_rate_limit(
            "scraper_api_test",
            underway::rate_limit::RateLimitAlgorithm::FixedWindow {
                max_tokens: 2,
                window: Duration::from_secs(4),
            },
        )
        .pool(pool.clone())
        .build()
        .await?;

    // Create 3 worker processes. They each default to ~num_cpus concurrent tasks.
    let w1 = Worker::new(heavy_queue.clone().into(), HeavyWorkTask);
    let w2 = Worker::new(heavy_queue.clone().into(), HeavyWorkTask);
    let w3 = Worker::new(web_queue.clone().into(), WebScraperTask);
    let w4 = Worker::new(web_queue.clone().into(), WebScraperTask);

    tokio::spawn(async move { w1.run().await });
    tokio::spawn(async move { w2.run().await });
    tokio::spawn(async move { w3.run().await });
    tokio::spawn(async move { w4.run().await });

    // Enqueue 10 Heavy CPU tasks.
    // They will be strictly gated at 2 active jobs max across w1 and w2.
    for i in 1..=5 {
        heavy_queue
            .enqueue(&pool, &HeavyWorkTask, &RunExpensiveJob { job_id: i })
            .await?;
    }

    // Enqueue 5 Web Scraper tasks.
    // They will be globally throttled by Postgres to EXACTLY 2 per 4 seconds,
    // even though w3 and w4 are capable of parallel processing hundreds instantly.
    for i in 1..=5 {
        web_queue
            .enqueue(
                &pool,
                &WebScraperTask,
                &ScrapeUrlJob {
                    url: format!("https://example.com/page-{}", i),
                },
            )
            .await?;
    }

    println!(
        "Waiting 15 seconds to observe distributed queues churning through limits natively..."
    );
    tokio::time::sleep(Duration::from_secs(15)).await;

    Ok(())
}
