use sqlx::{Executor, Pool, Postgres};
use async_trait::async_trait;
use anyhow::Result;
use uuid::{Uuid};
use quaint::prelude::{Insert, SingleRowInsert, Update, default_value};
use chrono::{DateTime, Utc};
use crate::postgres::{PostgresBaseEntityData, fetch_one, update_and_fetch_one, BurchillPostgresError};


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

    fn create_insert_query<'b>(&self) -> Result<SingleRowInsert<'b>>;
    fn create_update_query<'b>(&self) -> Result<Update<'b>>;

    async fn post_save_hook(&mut self) -> Result<()> {
        Ok(())
    }
    async fn post_insert_hook(&mut self) -> Result<()> {
        Ok(())
    }
    async fn post_update_hook(&mut self) -> Result<()> {
        Ok(())
    }
    async fn pre_save_hook(&mut self) -> Result<()> {
        Ok(())
    }
    async fn pre_insert_hook(&mut self) -> Result<()> {
        Ok(())
    }
    async fn pre_update_hook(&mut self) -> Result<()> {
        Ok(())
    }


    async fn save<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), BurchillPostgresError>
    where E: Executor<'b, Database = Postgres> {
        if let Err(err) = self.pre_save_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }

        let result = if let Some(_) = self.get_id() {
            self.update(&user_id, executor)
        } else {
            self.insert(&user_id, executor)
        };

        let result = result.await?;

        if let Err(err) = self.post_save_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }
        
        Ok(result)
    }

    async fn insert<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), BurchillPostgresError>
    where E: Executor<'b, Database = Postgres> {
        if let Err(err) = self.pre_insert_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }

        let query = self.create_audited_insert_query(user_id)?;
        let query = Insert::from(query).returning(vec!["id", "created_by", "created_time", "active"]);

        let result: InsertReturn = fetch_one(query, executor).await?;

        let entity_manager = self.get_mutable_entity_manager();
        entity_manager.set_id(result.id);
        entity_manager.set_created_by(result.created_by);
        entity_manager.set_created_time(result.created_time);
        entity_manager.set_active(result.active);

        if let Err(err) = self.post_insert_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }

        Ok(())
    }

    async fn update<'b, E>(&mut self, user_id: &Uuid, executor: E) -> Result<(), BurchillPostgresError>
    where E: Executor<'b, Database = Postgres> {
        if let Err(err) = self.pre_update_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }

        let query = self.create_audited_update_query(user_id)?;
        let result: UpdateReturn = update_and_fetch_one(query, vec!["last_updated_by, last_updated_time"], executor).await?;

        let entity_manager = self.get_mutable_entity_manager();
        entity_manager.set_last_updated_by(result.last_updated_by);
        entity_manager.set_last_updated_time(result.last_updated_time);

        if let Err(err) = self.post_update_hook().await {
            return Err(BurchillPostgresError::AnyhowError(err));
        }

        Ok(())
    }
    
    async fn quicksave(&mut self, user_id: &Uuid) -> Result<(), BurchillPostgresError> {
        let pool = self.get_pool();
        self.save(user_id, pool).await?;
        Ok(())
    }
    
    fn create_audited_update_query<'b>(&self, user_id: &Uuid) -> Result<Update<'b>, BurchillPostgresError> {
        Ok(self.create_update_query()?
            .set("last_updated_time", Utc::now())
            .set("last_updated_by", user_id.to_owned()))
    }
    
    fn create_audited_insert_query<'b>(&self, user_id: &Uuid) -> Result<SingleRowInsert<'b>, BurchillPostgresError> {
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
