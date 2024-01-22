mod db;
mod job_utils;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tabby_db::DbConn;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::error;

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub enum JobType {
    SyncRepositories,
    IndexRepositories,
}

impl JobType {
    fn name(&self) -> &str {
        match self {
            JobType::SyncRepositories => "sync",
            JobType::IndexRepositories => "index",
        }
    }

    fn schedule(&self) -> &str {
        match self {
            // run every 5 minutes
            JobType::SyncRepositories => "0 1/5 * * * * *",
            // run every 5 hours
            JobType::IndexRepositories => "0 0 1/5 * * * *",
        }
    }
}

#[derive(Default)]
pub struct JobHooks {
    hooks: Mutex<HashMap<JobType, Vec<Box<dyn Fn() + Sync + Send>>>>,
}

impl JobHooks {
    pub fn add_hook(&self, job: JobType, hook: impl Fn() + 'static + Sync + Send) {
        let mut hooks = self.hooks.lock().unwrap();
        let entries = hooks.entry(job).or_default();
        entries.push(Box::new(hook))
    }

    pub fn run_hooks(&self, job: JobType) {
        let hooks = self.hooks.lock().unwrap();
        let Some(entries) = hooks.get(&job) else {
            return;
        };
        for entry in entries {
            entry();
        }
    }
}

async fn new_job_scheduler(jobs: Vec<Job>) -> anyhow::Result<JobScheduler> {
    let scheduler = JobScheduler::new().await?;
    for job in jobs {
        scheduler.add(job).await?;
    }
    scheduler.start().await?;
    Ok(scheduler)
}

pub fn run_cron(db_conn: &DbConn) -> Arc<JobHooks> {
    let hooks = Arc::new(JobHooks::default());
    let hooks_clone = hooks.clone();
    let db_conn = db_conn.clone();
    tokio::spawn(async move {
        let hooks = hooks_clone;
        let Ok(job1) = db::refresh_token_job(db_conn.clone()).await else {
            error!("failed to create db job");
            return;
        };
        let Ok(job2) =
            job_utils::run_job(db_conn.clone(), JobType::SyncRepositories, hooks.clone()).await
        else {
            error!("failed to create sync job");
            return;
        };
        let Ok(job3) =
            job_utils::run_job(db_conn.clone(), JobType::IndexRepositories, hooks.clone()).await
        else {
            error!("failed to create index job");
            return;
        };

        let Ok(mut scheduler) = new_job_scheduler(vec![job1, job2, job3]).await else {
            error!("failed to start job scheduler");
            return;
        };

        loop {
            match scheduler.time_till_next_job().await {
                Ok(Some(duration)) => {
                    tokio::time::sleep(duration).await;
                }
                Ok(None) => {
                    // wait until scheduler increases jobs' tick
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    error!("failed to get job sleep time: {}, re-try in 1 second", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
    hooks
}
