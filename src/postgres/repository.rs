use quaint::prelude::Select;
use sqlx::{Executor, Pool, Postgres};
use anyhow::Result;
use async_trait::async_trait;
use uuid::{Uuid};

#[async_trait]
pub trait PostgresRepository<'a, T> {
    fn new(pool: &'a Pool<Postgres>) -> Self;

    async fn find_one(&self, id: &Uuid) -> Result<T>;

    async fn find_one_with_executor<'b, E>(&self, id: &Uuid, executor: E) -> Result<T>
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
