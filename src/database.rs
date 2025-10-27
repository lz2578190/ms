use async_trait::async_trait;
use hbb_common::{log, ResultType};
use std::ops::DerefMut;
// use sqlx::{
//     sqlite::SqliteConnectOptions, ConnectOptions, Connection, Error as SqlxError, SqliteConnection,
// };
use sqlx::{
    self,
    sqlite::SqliteConnectOptions,
    ConnectOptions,
    SqliteConnection,
    Executor,
    Error as SqlxError,
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

use std::ops::DerefMut;
use sqlx::{self, Sqlite, SqlitePool, Executor}; // 视你现有 use 而定
// 其他 use 按你项目保持不变…

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
            DbPool { url: url.to_owned() },
            n,
        );
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    // ✅ 修正版：使用事务 + 运行期校验（query()），并正确持有连接的 guard
    async fn create_tables(&self) -> ResultType<()> {
        let mut guard = self.pool.get().await?;         // 持有连接 guard
        let conn = guard.deref_mut();                   // &mut SqliteConnection
        let mut tx = conn.begin().await?;               // 一个事务里建表 + 索引

        // peer 表
        sqlx::query(
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
            "#
        ).execute(&mut *tx).await?;

        sqlx::query("create unique index if not exists index_peer_id on peer (id)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_user on peer (user)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_created_at on peer (created_at)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_status on peer (status)")
            .execute(&mut *tx).await?;

        // ✅ 保留 license_bind （注意：created_at 用 integer 存毫秒时间戳）
        sqlx::query(
            r#"
            create table if not exists license_bind (
                id   varchar(100) primary key,
                note varchar(300),
                created_at integer not null
            ) without rowid;
            "#
        ).execute(&mut *tx).await?;

        sqlx::query("create index if not exists index_license_bind_id on license_bind (id)")
            .execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    // 其余已有的 peer 相关函数不变。下面给出 license_bind 的 CRUD（全部使用 query()+bind()）

    // 写入/更新 白名单
    pub async fn upsert_license_bind(&self, id: &str, note: &str, now_ms: i64) -> ResultType<()> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();
        sqlx::query(
            r#"
            insert into license_bind(id, note, created_at)
            values(?, ?, ?)
            on conflict(id) do update set
                note = excluded.note,
                created_at = excluded.created_at
            "#
        )
        .bind(id)
        .bind(note)
        .bind(now_ms)
        .execute(&mut *conn)
        .await?;
        Ok(())
    }

    // 读取创建时间（可选）
    pub async fn get_license_bind_created_at(&self, id: &str) -> ResultType<Option<i64>> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();
        let row = sqlx::query_scalar::<_, i64>(
            "select created_at from license_bind where id = ? limit 1"
        )
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;
        Ok(row)
    }

    // 是否存在
    pub async fn license_bind_exists(&self, id: &str) -> ResultType<bool> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();
        let cnt = sqlx::query_scalar::<_, i64>(
            "select count(1) from license_bind where id = ?"
        )
        .bind(id)
        .fetch_one(&mut *conn)
        .await?;
        Ok(cnt > 0)
    }

    // 删除
    pub async fn delete_license_bind(&self, id: &str) -> ResultType<()> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();
        sqlx::query("delete from license_bind where id = ?")
            .bind(id)
            .execute(&mut *conn)
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
