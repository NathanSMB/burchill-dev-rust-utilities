use quaint::prelude::Select;
use sqlx::{Executor, Postgres};
use anyhow::Result;
use async_trait::async_trait;
use uuid::{Uuid};

#[async_trait]
pub trait PostgresRepository<T> {
    fn new() -> Self;

    async fn find_one<'b, E>(&self, executor: E, id: &Uuid) -> Result<T>
    where E: Executor<'b, Database = Postgres>;
}

pub fn add_base_fields_to_select(query: Select) -> Select {
    query
        .column("id")
        .column("created_time")
        .column("created_by")
        .column("last_updated_time")
        .column("last_updated_by")
        .column("active")
}
