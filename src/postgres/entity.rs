use sqlx::{Executor, Pool, Postgres};
use async_trait::async_trait;
use uuid::{Uuid};
use std::{error, fmt};
use quaint::prelude::{Insert, SingleRowInsert, Update, default_value};
use thiserror::Error;
use chrono::{DateTime, Utc};
use crate::postgres::{PostgresBaseEntityData, fetch_one, update_and_fetch_one};


#[derive(Clone)]
pub struct PostgresEntityManager<'a> {
    pub entity_data: PostgresBaseEntityData,
    pub pool: &'a Pool<Postgres>
}

impl<'a> PostgresEntityManager<'a> {
    pub fn new(pool: &'a Pool<Postgres>) -> Self {
        PostgresEntityManager {
            entity_data: PostgresBaseEntityData {
                id: None,
                created_time: None,
                created_by: None,
                last_updated_by: None,
                last_updated_time: None,
                active: None
            },
            pool
        }
    }

    pub fn from_db(pool: &'a Pool<Postgres>, data: PostgresBaseEntityData) -> Self {
        PostgresEntityManager {
            entity_data: data,
            pool
        }
    }

    fn get_pool(&self) -> &'a Pool<Postgres> {
        &self.pool
    }

    fn get_id(&self) -> Option<Uuid> {
        self.entity_data.id.to_owned()
    }

    fn set_id(&mut self, id: Uuid) {
        if let None = self.entity_data.id {
            self.entity_data.id = Some(id);
        }
    }

    fn get_created_time(&self) -> Option<DateTime<Utc>> {
        self.entity_data.created_time.to_owned()
    }

    fn set_created_time(&mut self, time: DateTime<Utc>) {
        if let None = self.entity_data.created_time {
            self.entity_data.created_time = Some(time);
        }
    }

    fn get_last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.entity_data.last_updated_time.to_owned()
    }

    fn set_last_updated_time(&mut self, time: DateTime<Utc>) {
        self.entity_data.last_updated_time = Some(time);
    }

    fn get_created_by(&self) -> Option<Uuid> {
        self.entity_data.created_by.to_owned()
    }

    fn set_created_by(&mut self, user_id: Uuid) {
        if let None = self.entity_data.created_by {
            self.entity_data.created_by = Some(user_id);
        }
    }

    fn get_last_updated_by(&self) -> Option<Uuid> {
        self.entity_data.last_updated_by.to_owned()
    }

    fn set_last_updated_by(&mut self, user_id: Uuid) {
        self.entity_data.last_updated_by = Some(user_id);
    }

    fn get_active(&self) -> Option<bool> {
        self.entity_data.active.to_owned()
    }

    fn set_active(&mut self, active: bool) {
        self.entity_data.active = Some(active)
    }
}

#[async_trait]
pub trait PostgresEntity<'a, D> {
    fn new(pool: &'a Pool<Postgres>, data: D) -> Self;
    fn from_db(data: D, manager: PostgresEntityManager<'a>) -> Self;

    fn get_entity_manager(&self) -> &PostgresEntityManager<'a>;
    fn get_mutable_entity_manager(&mut self) -> &mut PostgresEntityManager<'a>;

    fn get_pool(&self) -> &'a Pool<Postgres> {
        self.get_entity_manager().get_pool()
    }

    fn get_id(&self) -> Option<Uuid> {
        self.get_entity_manager().get_id()
    }

    fn get_created_time(&self) -> Option<DateTime<Utc>> {
        self.get_entity_manager().get_created_time()
    }

    fn get_last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.get_entity_manager().get_last_updated_time()
    }

    fn get_created_by(&self) -> Option<Uuid> {
        self.get_entity_manager().get_created_by()
    }

    fn get_last_updated_by(&self) -> Option<Uuid> {
        self.get_entity_manager().get_last_updated_by()
    }

    fn get_active(&self) -> Option<bool> {
        self.get_entity_manager().get_active()
    }

    fn set_active(&mut self, active: bool) {
        self.get_mutable_entity_manager().set_active(active);
    }

    fn create_insert_query<'b>(&self) -> Result<SingleRowInsert<'b>, Box<dyn error::Error>>;
    fn create_update_query<'b>(&self) -> Result<Update<'b>, Box<dyn error::Error>>;

    async fn post_save_hook(&mut self) -> Result<(), Box<dyn error::Error>> {
        Ok(())
    }

    async fn save<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), Box<dyn error::Error>>
    where E: Executor<'b, Database = Postgres> {
        let result = if let Some(_) = self.get_id() {
            self.update(&user_id, executor)
        } else {
            self.insert(&user_id, executor)
        };
        
        Ok(result.await?)
    }

    async fn insert<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), Box<dyn error::Error>>
    where E: Executor<'b, Database = Postgres> {
        let query = self.create_audited_insert_query(user_id)?;
        let query = Insert::from(query).returning(vec!["id", "created_by", "created_time", "active"]);

        let result: InsertReturn = fetch_one(query, executor).await?;

        let entity_manager = self.get_mutable_entity_manager();
        entity_manager.set_id(result.id);
        entity_manager.set_created_by(result.created_by);
        entity_manager.set_created_time(result.created_time);
        entity_manager.set_active(result.active);

        self.post_save_hook().await
    }

    async fn update<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), Box<dyn error::Error>>
    where E: Executor<'b, Database = Postgres> {
        let query = self.create_audited_update_query(user_id)?;
        let result: UpdateReturn = update_and_fetch_one(query, vec!["last_updated_by, last_updated_time"], executor).await?;

        let entity_manager = self.get_mutable_entity_manager();
        entity_manager.set_last_updated_by(result.last_updated_by);
        entity_manager.set_last_updated_time(result.last_updated_time);

        self.post_save_hook().await
    }
    
    async fn quicksave(&mut self, user_id: &Uuid) -> Result<(), Box<dyn error::Error>> {
        let pool = self.get_pool();
        self.save(user_id, pool).await?;
        Ok(())
    }
    
    fn create_audited_update_query<'b>(&self, user_id: &Uuid) -> Result<Update<'b>, Box<dyn error::Error>> {
        Ok(self.create_update_query()?
            .set("last_updated_time", Utc::now())
            .set("last_updated_by", user_id.to_owned()))
    }
    
    fn create_audited_insert_query<'b>(&self, user_id: &Uuid) -> Result<SingleRowInsert<'b>, Box<dyn error::Error>> {
        Ok(self.create_insert_query()?
            .value("created_time", default_value())
            .value("created_by", user_id.to_owned()))
    }
}
#[derive(sqlx::FromRow)]
struct InsertReturn {
    id: Uuid,
    created_by: Uuid,
    created_time: DateTime<Utc>,
    active: bool
}

#[derive(sqlx::FromRow)]
struct UpdateReturn {
    last_updated_by: Uuid,
    last_updated_time: DateTime<Utc>
}


#[derive(Debug, Clone)]
pub struct EntityMissingIdError;

impl error::Error for EntityMissingIdError {}

impl fmt::Display for EntityMissingIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entity is missing it's primary id when it was required.")
    }
}

#[derive(Debug, Clone)]
pub struct UnknownSqlType;

impl error::Error for UnknownSqlType {}

impl fmt::Display for UnknownSqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not determine a values SQL type before binding.")
    }
}
