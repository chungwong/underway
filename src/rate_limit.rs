//! # Globally Rate Limiting Queues
//!
//! The concept of Rate Limiting in Underway was split into two highly optimized
//! paradigms: **Local Concurrency Control** and **Global API Throttling**.
//!
//! ## 1. Local Pool Control (`max_concurrency`)
//!
//! If you just need to ensure your worker pool doesn't crash your server by
//! processing 1,000 tasks at the exact same instant, use `max_concurrency`.
//!
//! This is a fast, database-native lock that simply ensures no more than `N`
//! tasks are in the `InProgress` state on the table at any given time.
//!
//! ```rust,no_run
//! # use sqlx::{Postgres, Transaction};
//! # use underway::{Task, task::Result as TaskResult, Queue};
//! # struct HeavyCpuTask;
//! # impl Task for HeavyCpuTask {
//! #     type Input = ();
//! #     type Output = ();
//! #     async fn execute(
//! #         &self,
//! #         _: Transaction<'_, Postgres>,
//! #         _: Self::Input,
//! #     ) -> TaskResult<Self::Output> {
//! #         Ok(())
//! #     }
//! # }
//! # async fn example(pool: sqlx::PgPool) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let queue: Queue<HeavyCpuTask> = Queue::builder()
//!     .name("heavy-cpu-tasks")
//!     .max_concurrency(5) // Never run more than 5 at a time
//!     .pool(pool)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## 2. Global Distributed Rate Throttling
//!
//! Often, tasks hit an external SaaS API (like Stripe or Spotify) that enforces
//! strict rate limits like **"Max 100 requests per minute"**. Using
//! `max_concurrency` here doesn't work, because you could technically process
//! 100 tasks within 2 seconds (concurrency 5) and still trigger a 429 Too Many
//! Requests ban from the API.
//!
//! Instead, you need a true global rate limit logic that behaves as a Token
//! Bucket. Because you likely run multiple horizontal worker servers, the state
//! of this limit *must* be shared.
//!
//! Underway now offers a completely native, out-of-the-box solution using
//! PostgreSQL itself as the global state mechanism:
//!
//! ```rust,no_run
//! # use sqlx::{Postgres, Transaction};
//! # use underway::{Task, task::Result as TaskResult, Queue};
//! # use std::time::Duration;
//! # struct StripeApiTask;
//! # impl Task for StripeApiTask {
//! #     type Input = ();
//! #     type Output = ();
//! #     async fn execute(
//! #         &self,
//! #         _: Transaction<'_, Postgres>,
//! #         _: Self::Input,
//! #     ) -> TaskResult<Self::Output> {
//! #         Ok(())
//! #     }
//! # }
//! # async fn example(pool: sqlx::PgPool) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let queue: Queue<StripeApiTask> = Queue::builder()
//!     .name("stripe-api-jobs")
//!     // Native Postgres Rate Limiting
//!     // "Bucket ID: stripe_api" -> 100 requests / 60 seconds
//!     .global_rate_limit(
//!         "stripe_api",
//!         underway::rate_limit::RateLimitAlgorithm::FixedWindow {
//!             max_tokens: 100,
//!             window: Duration::from_secs(60),
//!         }
//!     )
//!     .pool(pool)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! By simply supplying those numbers, Underway generates a `RateLimiter` plugin
//! that tracks tokens in the `underway.global_rate_limit` table. Every single
//! container you boot up will flawlessly coordinate together so that the
//! summation of their throughput never exceeds 100 hits per 60 seconds.
//!
//! ### Why this saves massive CPU/Database load (No "Busy-Waiting"):
//!
//! When a worker fetches a task and realizes the global rate limit bucket is
//! currently empty, it receives exactly how many milliseconds are left until
//! the next token is mathematically available.
//!
//! Instead of executing the task and violating the downstream limit, or
//! crashing the task and causing noisy Retry loops, the worker intercepts the
//! task and dynamically postpones it in memory:
//!
//! The worker simply sleeps the specific Task Future for `X` milliseconds
//! locally, and then seamlessly proceeds to execute it once the exact bucket
//! timestamp clears, without yielding the task back to PostgreSQL!
//!
//! This means the web workers and the PostgreSQL system undergo **0 compute
//! cost** waiting for the bucket to refill, and tasks are executed natively at
//! the exact maximum allowable throughput of the API!
//!
//! ### Managing the Bucket State
//! Underway deliberately avoids exposing raw database internals to consumers.
//! If you need to manually intervene (e.g., during tests or because a bucket
//! became stuck), you should use the provided helper rather than writing raw
//! `DELETE` SQL queries:
//!
//! ```rust,no_run
//! # use sqlx::{Postgres, Transaction};
//! # use underway::{Task, task::Result as TaskResult, Queue};
//! # async fn example(pool: sqlx::PgPool) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! // Safely resets the tokens for this specific bucket
//! underway::rate_limit::clear(&pool, "stripe_api").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Implementing Industry-Standard Token Buckets
//! In addition to Fixed Windows, the true native `TokenBucket` algorithm is
//! perfectly supported! If you want "smooth" execution (e.g. consistently
//! dripping 2 tasks per second) but want to allow a short burst of up to 10
//! tokens:
//!
//! ```rust,no_run
//! # use sqlx::{Postgres, Transaction};
//! # use underway::{Task, task::Result as TaskResult, Queue};
//! # struct StripeApiTask;
//! # impl Task for StripeApiTask {
//! #     type Input = ();
//! #     type Output = ();
//! #     async fn execute(
//! #         &self,
//! #         _: Transaction<'_, Postgres>,
//! #         _: Self::Input,
//! #     ) -> TaskResult<Self::Output> {
//! #         Ok(())
//! #     }
//! # }
//! # async fn example(pool: sqlx::PgPool) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let queue: Queue<StripeApiTask> = Queue::builder()
//!     .name("openai-api-jobs")
//!     .global_rate_limit(
//!         "openai_api",
//!         underway::rate_limit::RateLimitAlgorithm::TokenBucket {
//!             max_capacity: 10.0,
//!             refill_rate_per_sec: 2.0,
//!         }
//!     )
//!     .pool(pool)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Generic Cell Rate Algorithm (GCRA)
//! If you want the smooth execution of a Token Bucket but want maximum database
//! performance via a 1-column tracking state (`Theoretical Arrival Time`), use
//! `GCRA`:
//!
//! ```rust,no_run
//! # use sqlx::{Postgres, Transaction};
//! # use underway::{Task, task::Result as TaskResult, Queue};
//! # use std::time::Duration;
//! # struct DiscordApiTask;
//! # impl Task for DiscordApiTask {
//! #     type Input = ();
//! #     type Output = ();
//! #     async fn execute(
//! #         &self,
//! #         _: Transaction<'_, Postgres>,
//! #         _: Self::Input,
//! #     ) -> TaskResult<Self::Output> {
//! #         Ok(())
//! #     }
//! # }
//! # async fn example(pool: sqlx::PgPool) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! let queue: Queue<DiscordApiTask> = Queue::builder()
//!     .name("discord-api-jobs")
//!     .global_rate_limit(
//!         "discord_api",
//!         underway::rate_limit::RateLimitAlgorithm::GCRA {
//!             // Wait mathematically 1 second between every task
//!             emission_interval: Duration::from_secs(1),
//!             // Allow a burst 5 seconds "early" (essentially 5 burst iterations)
//!             delay_variation_tolerance: Duration::from_secs(5),
//!         }
//!     )
//!     .pool(pool)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::{future::Future, pin::Pin, time::Duration};

use sqlx::PgPool;

use crate::task::{Error, RateLimitDecision, RateLimiter, Result};

/// The specific algorithm and configuration used by the distributed rate
/// limiter.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RateLimitAlgorithm {
    /// A Fixed Window algorithm that resets exactly `window` duration after the
    /// first request.
    ///
    /// This perfectly caps total throughput within the defined window, making
    /// it ideal for adhering to remote HTTP APIs with "100 requests per
    /// minute" type constraints.
    FixedWindow {
        /// The maximum number of tasks allowed to process within the window.
        max_tokens: i32,
        /// The exact duration of the fixed limit window.
        window: Duration,
    },
    /// A token bucket algorithm where tasks consume tokens that refill at a
    /// constant rate over time.
    ///
    /// This provides perfectly smooth execution (e.g., exactly 2 tasks per
    /// second) while allowing for short concurrency bursts up to the
    /// `max_capacity`.
    TokenBucket {
        /// The maximum burst capacity of the bucket (the maximum tokens that
        /// can be accumulated).
        max_capacity: f64,
        /// How rapidly the bucket refills (tokens per second).
        refill_rate_per_sec: f64,
    },
    /// Generic Cell Rate Algorithm (GCRA).
    /// Mathematically equivalent to a Token Bucket but tracks state natively in
    /// PostgreSQL using only a single Theoretical Arrival Time (TAT)
    /// timestamp for ultimate efficiency.
    GCRA {
        /// The cost of a single task (e.g., 1 task per 500ms).
        emission_interval: Duration,
        /// The maximum burst tolerance. A value of 0 means strict rate spacing.
        /// A value of `emission_interval * 10` would allow an immediate burst
        /// of 10 tasks.
        delay_variation_tolerance: Duration,
    },
}

/// A native rate limiter that uses PostgreSQL to track and enforce a global
/// rate limit. Because it uses a centralized database, limits are strictly
/// enforced across all independent distributed workers.
#[derive(Debug, Clone)]
pub struct PostgresRateLimiter {
    pool: PgPool,
    /// The unique identifier/bucket for this limit (e.g. `"stripe_api"`).
    id: String,
    /// The specific rate limiting strategy to execute.
    algorithm: RateLimitAlgorithm,
}

impl PostgresRateLimiter {
    /// Creates a new Postgres-backed global rate limiter.
    pub fn new(pool: PgPool, id: impl Into<String>, algorithm: RateLimitAlgorithm) -> Self {
        Self {
            pool,
            id: id.into(),
            algorithm,
        }
    }

    async fn acquire_lock<'a>(&self, tx: &mut sqlx::Transaction<'a, sqlx::Postgres>) -> Result<()> {
        // Serialize access to this specific rate limit bucket across all workers
        sqlx::query!("SELECT pg_advisory_xact_lock(hashtext($1))", self.id)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Retryable(e.to_string()))?;
        Ok(())
    }
}

impl RateLimiter for PostgresRateLimiter {
    fn check<'a>(
        &'a self,
        _queue_name: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RateLimitDecision>> + Send + 'a>> {
        Box::pin(async {
            match &self.algorithm {
                RateLimitAlgorithm::FixedWindow { max_tokens, window } => {
                    let mut tx = self
                        .pool
                        .begin()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    self.acquire_lock(&mut tx).await?;

                    // Convert Rust duration to Postgres interval friendly format (seconds)
                    let window_secs = window.as_secs_f64();

                    // Fetch the current state of the bucket, resetting it if the time window has
                    // expired
                    let row = sqlx::query!(
                        r#"
                        INSERT INTO underway.global_rate_limit (id, tokens_used, window_start)
                        VALUES ($1, 1, now())
                        ON CONFLICT (id) DO UPDATE
                        SET
                            tokens_used = CASE
                                WHEN extract(epoch from (now() - underway.global_rate_limit.window_start))::float8 >= $2 THEN 1
                                ELSE underway.global_rate_limit.tokens_used + 1
                            END,
                            window_start = CASE
                                WHEN extract(epoch from (now() - underway.global_rate_limit.window_start))::float8 >= $2 THEN now()
                                ELSE underway.global_rate_limit.window_start
                            END
                        RETURNING tokens_used, extract(epoch from (now() - window_start))::float8 as "elapsed_secs!"
                        "#,
                        self.id,
                        window_secs
                    )
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| Error::Retryable(e.to_string()))?;

                    tx.commit()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    let tokens_used = row.tokens_used as f64;
                    let elapsed_secs = row.elapsed_secs;

                    if tokens_used > *max_tokens as f64 {
                        let remaining_secs = window_secs - elapsed_secs;

                        let wait_time = Duration::from_secs_f64(remaining_secs.max(0.0));

                        Ok(RateLimitDecision::Limited {
                            retry_after: wait_time,
                        })
                    } else {
                        Ok(RateLimitDecision::Allowed)
                    }
                }
                RateLimitAlgorithm::TokenBucket {
                    max_capacity,
                    refill_rate_per_sec,
                } => {
                    let mut tx = self
                        .pool
                        .begin()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    self.acquire_lock(&mut tx).await?;

                    // TokenBucket logic via Postgres Upsert:
                    // `tokens_used` maps to `tokens_consumed`.
                    // We calculate tokens newly generated since `window_start` and subtract them
                    // from `tokens_used`, clamped horizontally by 0 and max_capacity.
                    let row = sqlx::query!(
                        r#"
                        WITH current_state AS (
                            SELECT tokens_used, window_start FROM underway.global_rate_limit WHERE id = $1
                        ),
                        calculation AS (
                            SELECT
                                -- For a fresh bucket, current_state is empty! `LEAST(max, NULL)` evaluates to `max` in postgres, causing
                                -- the query to bloat the bucket instantly to max_capacity.
                                -- Use an explicit IF/COALESCE guard on the entire calculation!
                                COALESCE(
                                    (SELECT GREATEST(0.0, LEAST($3::float8, tokens_used - extract(epoch from (now() - window_start))::float8 * $2)) FROM current_state),
                                    0.0
                                ) AS current_tokens,
                                now() AS arrival_time
                        ),
                        decision AS (
                            SELECT
                                current_tokens,
                                arrival_time,
                                (current_tokens + 1.0 <= $3::float8) AS is_allowed
                            FROM calculation
                        ),
                        upsert AS (
                            INSERT INTO underway.global_rate_limit (id, tokens_used, window_start)
                            SELECT $1, 1.0, arrival_time FROM decision WHERE is_allowed = true
                            ON CONFLICT (id) DO UPDATE
                            SET
                                tokens_used = (SELECT current_tokens FROM decision) + 1.0,
                                window_start = (SELECT arrival_time FROM decision)
                            WHERE (SELECT is_allowed FROM decision) = true
                        )
                        SELECT
                            is_allowed AS "is_allowed!",
                            current_tokens AS "current_tokens!"
                        FROM decision
                        "#,
                        self.id,
                        refill_rate_per_sec,
                        max_capacity
                    )
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| Error::Retryable(e.to_string()))?;

                    tx.commit()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    if row.is_allowed {
                        Ok(RateLimitDecision::Allowed)
                    } else {
                        let deficit = (row.current_tokens + 1.0) - *max_capacity;
                        let wait_secs = deficit / *refill_rate_per_sec;
                        let wait_time = std::time::Duration::from_secs_f64(wait_secs.max(0.0));
                        Ok(RateLimitDecision::Limited {
                            retry_after: wait_time,
                        })
                    }
                }
                RateLimitAlgorithm::GCRA {
                    emission_interval,
                    delay_variation_tolerance,
                } => {
                    let mut tx = self
                        .pool
                        .begin()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    self.acquire_lock(&mut tx).await?;

                    let emission_secs = emission_interval.as_secs_f64();
                    let tolerance_secs = delay_variation_tolerance.as_secs_f64();

                    // GCRA Logic via Postgres Upsert:
                    // `window_start` maps to Theoretical Arrival Time (TAT).
                    // We only allow execution if the new TAT <= now() + delay_variation_tolerance.
                    // If allowed, we update the DB's TAT. If limited, we do NOT update the DB, and
                    // return wait time.
                    let row = sqlx::query!(
                        r#"
                        WITH current_state AS (
                            SELECT window_start AS tat FROM underway.global_rate_limit WHERE id = $1
                        ),
                        calculation AS (
                            SELECT
                                -- If this is a completely fresh bucket, start the TAT exactly 1 full emission interval into the past.
                                -- Use LEAST to ensure the current_tat bounds the GREATEST call correctly.
                                COALESCE((SELECT tat FROM current_state), now() - make_interval(secs => $2)) AS current_tat,
                                now() AS arrival_time
                        ),
                        decision AS (
                            SELECT
                                current_tat,
                                arrival_time,
                                -- If current_tat is in the past, jump it to exactly `arrival_time`.
                                -- But since we initialized an empty bucket directly to `now() - emission`,
                                -- the GREATEST call would jump it to `now()` here!
                                -- So for a fresh bucket, new_tat = now() + emission
                                GREATEST(arrival_time - make_interval(secs => $3), current_tat) + make_interval(secs => $2) AS new_tat,
                                arrival_time + make_interval(secs => $3) AS allowance_time
                            FROM calculation
                        ),
                        upsert AS (
                            INSERT INTO underway.global_rate_limit (id, tokens_used, window_start)
                            SELECT $1, 0, new_tat FROM decision
                            ON CONFLICT (id) DO UPDATE
                            SET window_start = CASE
                                -- Only advance the TAT tracker if the resulting token request fits within the delay tolerance burst!
                                WHEN (SELECT new_tat FROM decision) <= (SELECT allowance_time FROM decision)
                                THEN (SELECT new_tat FROM decision)
                                ELSE underway.global_rate_limit.window_start
                            END
                            RETURNING window_start
                        )
                        SELECT
                            -- Ensure we treat EXACT matches within the tolerance mathematically valid
                            (new_tat <= allowance_time) AS "is_allowed!",
                            -- Wait time is always how far the target TAT overshoots the burst tolerance allowance limit
                            extract(epoch from (new_tat - allowance_time))::float8 AS "wait_time_secs!"
                        FROM decision
                        "#,
                        self.id,
                        emission_secs,
                        tolerance_secs
                    )
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| Error::Retryable(e.to_string()))?;

                    tx.commit()
                        .await
                        .map_err(|e| Error::Retryable(e.to_string()))?;

                    if row.is_allowed {
                        Ok(RateLimitDecision::Allowed)
                    } else {
                        let wait_time =
                            std::time::Duration::from_secs_f64(row.wait_time_secs.max(0.0));
                        Ok(RateLimitDecision::Limited {
                            retry_after: wait_time,
                        })
                    }
                }
            }
        })
    }
}

/// Resets the global rate limit state for a specified bucket ID.
///
/// This safely removes the tracking state for the given `id` in the
/// `underway.global_rate_limit` table. It is primarily designed for use in
/// system testing or to manually clear a stuck bucket.
pub async fn clear<'a, E>(executor: E, id: &str) -> std::result::Result<(), sqlx::Error>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    sqlx::query!(
        r#"
        DELETE FROM underway.global_rate_limit
        WHERE id = $1
        "#,
        id
    )
    .execute(executor)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::task::RateLimitDecision;

    async fn assert_rate_limiter_behavior(
        pool: &sqlx::PgPool,
        key: &str,
        algorithm: RateLimitAlgorithm,
        capacity: usize,
        expected_wait_min: u64,
        expected_wait_max: u64,
    ) {
        crate::rate_limit::clear(pool, key).await.unwrap();
        let limiter = PostgresRateLimiter::new(pool.clone(), key, algorithm);

        // 1. Consume the allowed capacity entirely
        for _ in 0..capacity {
            let res = limiter.check("test-rate-limit-queue").await.unwrap();
            assert_eq!(res, RateLimitDecision::Allowed);
        }

        // 2. The next request MUST be blocked
        let blocked = limiter.check("test-rate-limit-queue").await.unwrap();
        match blocked {
            RateLimitDecision::Limited { retry_after } => {
                assert!(
                    retry_after.as_secs() >= expected_wait_min
                        && retry_after.as_secs() <= expected_wait_max,
                    "Wait time {}s was not between {}s and {}s",
                    retry_after.as_secs(),
                    expected_wait_min,
                    expected_wait_max
                );

                // 3. Wait for the precise lockout duration (plus a tiny buffer)
                tokio::time::sleep(retry_after + Duration::from_millis(50)).await;

                // 4. The subsequent request MUST be allowed
                let recovered = limiter.check("test-rate-limit-queue").await.unwrap();
                assert_eq!(recovered, RateLimitDecision::Allowed);
            }
            _ => panic!("Expected request to be RateLimited! Got: {:?}", blocked),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn generates_valid_postgres_global_rate_limit_fixed_window(pool: sqlx::PgPool) {
        assert_rate_limiter_behavior(
            &pool,
            "test_api_fw",
            RateLimitAlgorithm::FixedWindow {
                max_tokens: 2,
                window: Duration::from_secs(10),
            },
            2,
            8,
            10,
        )
        .await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn generates_valid_postgres_global_rate_limit_token_bucket(pool: sqlx::PgPool) {
        assert_rate_limiter_behavior(
            &pool,
            "test_api_tb",
            RateLimitAlgorithm::TokenBucket {
                max_capacity: 2.0,
                refill_rate_per_sec: 0.1, // 1 token every 10 seconds
            },
            2,
            9, // Wait time mathematically calculates exactly deficit wait minus execute time
            10,
        )
        .await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn generates_valid_postgres_global_rate_limit_gcra(pool: sqlx::PgPool) {
        assert_rate_limiter_behavior(
            &pool,
            "test_api_gcra",
            RateLimitAlgorithm::GCRA {
                emission_interval: Duration::from_secs(10),
                delay_variation_tolerance: Duration::from_secs(12),
            },
            2,
            7, // GCRA wait formula subtracts from arrival, can drop lower based on CPU offset!
            12,
        )
        .await;
    }
}
