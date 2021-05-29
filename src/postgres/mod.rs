use sqlx::{Executor, FromRow, Pool, Postgres, postgres::{PgArguments, PgConnectOptions, PgPoolOptions, PgRow}, query::{QueryAs}};
use quaint::{Value, prelude::{Select, Update}, visitor::Visitor};
use thiserror::Error;
use chrono::{DateTime, Utc};
use uuid::{Uuid};

pub mod entity;
pub mod repository;


#[derive(Clone)]
pub struct PostgresBaseEntityData {
    pub id: Option<Uuid>,
    pub created_time: Option<DateTime<Utc>>,
    pub created_by: Option<Uuid>,
    pub last_updated_time: Option<DateTime<Utc>>,
    pub last_updated_by: Option<Uuid>,
    pub active: Option<bool>,
}

pub async fn get_connection_pool(options: PgConnectOptions, max_connections: u32) -> Result<Pool<Postgres>, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options).await?;
    Ok(pool)
}

pub fn add_bindings_to_query<'b, T>(query: QueryAs<'b, Postgres, T, PgArguments>, params: Vec<Value>) -> Result<QueryAs<'b, Postgres, T, PgArguments>, BurchillPostgresError> {
    let mut new_query = query;
    for value in params.into_iter() {
        new_query = add_binding_to_query(new_query, value)?;
    }
    Ok(new_query)
}

pub fn add_binding_to_query<'b, T>(query: QueryAs<'b, Postgres, T, PgArguments>, value: Value) -> Result<QueryAs<'b, Postgres, T, PgArguments>, BurchillPostgresError> {
    match value {
        Value::Integer(_) => Ok(query.bind(value.as_i64())),
        Value::Float(_) => Ok(query.bind(value.as_f32())),
        Value::Double(_) => Ok(query.bind(value.as_f64())),
        Value::Text(_) => Ok(query.bind(value.into_string())),
        // Value::Char(_) => Ok(query.bind(value.as_char())),
        Value::Boolean(_) => Ok(query.bind(value.as_bool())),
        // Value::Bytes(_) => Ok(query.bind(value.as_bytes())),
        // Value::Array() => Ok(query.bind(value.into_vec())),
        Value::Enum(_) => Ok(query.bind(value.into_string())),
        Value::Uuid(_) => Ok(query.bind(value.as_uuid())),
        Value::DateTime(_) => Ok(query.bind(value.as_datetime())),
        _ => Err(BurchillPostgresError::UnknownSqlType)
    }
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

pub fn create_sqlx_query<'a, T>(query: &'a str, bindings: Vec<Value>) -> Result<QueryAs<'a, sqlx::Postgres, T, PgArguments>, BurchillPostgresError>
where
    T: for<'r> FromRow<'r, PgRow>
{
    let sqlx_query = sqlx::query_as::<Postgres, T>(query);
    add_bindings_to_query::<T>(sqlx_query, bindings)
}

pub async fn fetch_one<'a, T, Q, E>(query: Q, executor: E) -> Result<T, BurchillPostgresError>
where
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    Q: Into<quaint::prelude::Query<'a>>,
    E: Executor<'a, Database = Postgres>
{
    let (query, bindings) = match quaint::visitor::Postgres::build(query) {
        Ok(query_and_bindings) => query_and_bindings,
        Err(err) => return Err(BurchillPostgresError::QuaintError(err))
    };

    println!("{}", query);

    let query = create_sqlx_query::<T>(query.as_str(), bindings)?;
    match query.fetch_one(executor).await {
        Ok(result) => Ok(result),
        Err(err) => Err(BurchillPostgresError::SqlxError(err))
    }
}

// Since quaint does not allow returns on an update query I have to hack it in! 🪓🪓🪓
pub async fn update_and_fetch_one<'a, T, E>(query: Update<'a>, returning_values: Vec<&str>, executor: E) -> Result<T, BurchillPostgresError> 
where 
    T: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    E: Executor<'a, Database = Postgres>
{
    let (mut query, bindings) = match quaint::visitor::Postgres::build(query) {
        Ok(query_and_bindings) => query_and_bindings,
        Err(err) => return Err(BurchillPostgresError::QuaintError(err))
    };

    if returning_values.len() > 0 {
        let mut count = 0;

        for value in returning_values.into_iter() {
            if count == 0 {
                query.push_str(" RETURNING ");
                count += 1;
            } else {
                query.push_str(", ");
                count += 1;
            }

            query.push_str(value);
        }
    }

    let query = create_sqlx_query(query.as_str(), bindings)?;
    match query.fetch_one(executor).await {
        Ok(result) => Ok(result),
        Err(err) => Err(BurchillPostgresError::SqlxError(err))
    }
}

#[derive(Error, Debug)]
pub enum BurchillPostgresError {
    #[error("Could not determine a values SQL type before binding.")]
    UnknownSqlType,
    #[error(transparent)]
    QuaintError(#[from] quaint::error::Error),
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error)
}
