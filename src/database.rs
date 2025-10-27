use async_trait::async_trait;
use hbb_common::{log, ResultType};
use sqlx::{
    sqlite::SqliteConnectOptions, ConnectOptions, Connection, Error as SqlxError, SqliteConnection,
};
use std::{ops::DerefMut, str::FromStr};

type Pool = deadpool::managed::Pool<DbPool>;

pub struct DbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let mut opt = SqliteConnectOptions::from_str(&self.url).unwrap();
        opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}", n);
        let pool = Pool::new(
            DbPool {
                url: url.to_owned(),
            },
            n,
        );
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        sqlx::query!(
            r#"
            create table if not exists peer (
                guid blob primary key not null,
                id varchar(100) not null,
                uuid blob not null,
                pk blob not null,
                created_at datetime not null default(current_timestamp),
                user blob,
                status tinyint,
                note varchar(300),
                info text not null
            ) without rowid;
            create unique index if not exists index_peer_id on peer (id);
            create index if not exists index_peer_user on peer (user);
            create index if not exists index_peer_created_at on peer (created_at);
            create index if not exists index_peer_status on peer (status);

            -- 控制端白名单缓存（允许发起连接的控制端 Peer ID）
            create table if not exists license_bind (
                id   varchar(100) primary key,         -- 控制端 Peer ID（无空格）
                note varchar(300),
                created_at integer not null default (cast(strftime('%s','now') as integer)*1000)
            ) without rowid;
            create index if not exists index_license_bind_id on license_bind (id);
            "#
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        Ok(sqlx::query_as!(
            Peer,
            "select guid, id, uuid, pk, user, status, info from peer where id = ?",
            id
        )
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?)
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query!(
            "insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)",
            guid,
            id,
            uuid,
            pk,
            info
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        sqlx::query!(
            "update peer set id=?, pk=?, info=? where guid=?",
            id,
            pk,
            info,
            guid
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    /// 允许某个控制端（Peer ID）发起连接（写白名单，若已存在则刷新时间）
    pub async fn cache_license_bind(&self, id: &str, note: &str) -> ResultType<()> {
        let id = id.replace(' ', "");
        let now_ms: i64 = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()) as i64;
        sqlx::query!(
            "insert into license_bind(id, note, created_at) values(?, ?, ?)
             on conflict(id) do update set note=excluded.note, created_at=excluded.created_at",
            id,
            note,
            now_ms
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    /// 本地是否允许（命中白名单且未过期），ttl_ms 为缓存 TTL
    pub async fn is_controller_allowed(&self, id: &str, ttl_ms: i64) -> ResultType<bool> {
        let id = id.replace(' ', "");
        let rec = sqlx::query!(
            "select created_at from license_bind where id = ? limit 1",
            id
        )
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?;
        if let Some(row) = rec {
            let now_ms: i64 = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()) as i64;
            let created = row.created_at.unwrap_or(0);
            return Ok(now_ms - created <= ttl_ms);
        }
        Ok(false)
    }

    /// 旧接口：手动插入一次性白名单
    pub async fn license_bind_insert(&self, id: &str, note: &str) -> ResultType<()> {
        self.cache_license_bind(id, note).await
    }

    /// 旧接口：检查是否存在（不看 TTL）
    pub async fn is_id_whitelisted(&self, id: &str) -> ResultType<bool> {
        let id = id.replace(' ', "");
        let rec = sqlx::query!("select id from license_bind where id = ? limit 1", id)
            .fetch_optional(self.pool.get().await?.deref_mut())
            .await?;
        Ok(rec.is_some())
    }

    /// 删除许可
    pub async fn license_bind_remove(&self, id: &str) -> ResultType<()> {
        let id = id.replace(' ', "");
        sqlx::query!("delete from license_bind where id = ?", id)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("test.sqlite3").await.unwrap();
        let mut jobs = vec![];
        for i in 0..1000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                let _ = cloned.insert_peer(&id, &empty_vec, &empty_vec, "").await;
                let _ = cloned.cache_license_bind(&id, "t").await;
                let _ = cloned.is_controller_allowed(&id, 24*3600*1000).await;
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}
