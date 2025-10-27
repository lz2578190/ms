use async_trait::async_trait;
use hbb_common::{log, ResultType};

use sqlx::{Executor, Row, Error as SqlxError};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::SqliteConnection;
use sqlx::ConnectOptions;

use std::{ops::DerefMut, str::FromStr, result::Result as StdResult};
use chrono::Utc;

type Pool = deadpool::managed::Pool<DbPool>;

pub struct DbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;

    async fn create(&self) -> StdResult<SqliteConnection, SqlxError> {
        let mut opt = SqliteConnectOptions::from_str(&self.url).unwrap();
        // 打开 SQL 打印（可按需关掉）
        opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }

    async fn recycle(&self, obj: &mut SqliteConnection) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

#[derive(Default, Debug, Clone)]
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
            let _ = std::fs::File::create(url);
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

        // test connection
        let _ = pool.get().await?;
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    /// 初始化表结构（事务 + 运行期执行）
    async fn create_tables(&self) -> ResultType<()> {
        let mut guard = self.pool.get().await?;       // guard 持有连接
        let conn = guard.deref_mut();                 // &mut SqliteConnection
        let mut tx = conn.begin().await?;             // 开事务

        // 1) peer 表
        sqlx::query(r#"
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
        "#).execute(&mut *tx).await?;

        sqlx::query("create unique index if not exists index_peer_id on peer (id)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_user on peer (user)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_created_at on peer (created_at)")
            .execute(&mut *tx).await?;
        sqlx::query("create index if not exists index_peer_status on peer (status)")
            .execute(&mut *tx).await?;

        // 2) license_bind（控制端 ID 白名单）
        sqlx::query(r#"
            create table if not exists license_bind (
                id   varchar(100) primary key,
                note varchar(300),
                created_at integer not null
            ) without rowid;
        "#).execute(&mut *tx).await?;

        sqlx::query("create index if not exists index_license_bind_id on license_bind (id)")
            .execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    // -------------------------
    // peer 相关（与原工程兼容）
    // -------------------------

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();

        let row_opt = sqlx::query(
            "select guid, id, uuid, pk, user, status, info from peer where id = ? limit 1"
        )
        .bind(id)
        .fetch_optional(&mut *conn)
        .await?;

        if let Some(row) = row_opt {
            let peer = Peer {
                guid:  row.get::<Vec<u8>, _>("guid"),
                id:    row.get::<String, _>("id"),
                uuid:  row.get::<Vec<u8>, _>("uuid"),
                pk:    row.get::<Vec<u8>, _>("pk"),
                user:  row.try_get::<Option<Vec<u8>>, _>("user").unwrap_or(None),
                status:row.try_get::<Option<i64>, _>("status").unwrap_or(None),
                info:  row.get::<String, _>("info"),
            };
            Ok(Some(peer))
        } else {
            Ok(None)
        }
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();

        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();

        sqlx::query(
            "insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)"
        )
        .bind(&guid)
        .bind(id)
        .bind(uuid)
        .bind(pk)
        .bind(info)
        .execute(&mut *conn)
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
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();

        sqlx::query(
            "update peer set id = ?, pk = ?, info = ? where guid = ?"
        )
        .bind(id)
        .bind(pk)
        .bind(info)
        .bind(guid)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    // -------------------------
    // license_bind（白名单）
    // -------------------------

    /// 供业务层“缓存允控端 ID”的便捷方法（你测试里调用了这个名字）
    pub async fn cache_license_bind(&self, id: &str, note: &str) -> ResultType<()> {
        let now_ms = Utc::now().timestamp_millis();
        self.upsert_license_bind(id, note, now_ms).await
    }

    /// 供业务层判断“控制端是否允许”：
    /// - 存在记录且未过期（now - created_at <= ttl_ms）→ true
    /// - 否则 false
    pub async fn is_controller_allowed(&self, id: &str, ttl_ms: i64) -> ResultType<bool> {
        if let Some(created_at) = self.get_license_bind_created_at(id).await? {
            let now_ms = Utc::now().timestamp_millis();
            return Ok(now_ms - created_at <= ttl_ms);
        }
        Ok(false)
    }

    /// upsert（内部用）
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

    /// 取 created_at（内部用）
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

    /// 是否存在（有些地方喜欢直接判断存在性）
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

    /// 删除（可选）
    pub async fn delete_license_bind(&self, id: &str) -> ResultType<()> {
        let mut guard = self.pool.get().await?;
        let conn = guard.deref_mut();

        sqlx::query("delete from license_bind where id = ?")
            .bind(id)
            .execute(&mut *conn)
            .await?;

        Ok(())
    }

    /// 兼容其它调用名
    pub async fn is_id_whitelisted(&self, id: &str) -> ResultType<bool> {
        self.license_bind_exists(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                let _ = cloned.is_controller_allowed(&id, 24 * 3600 * 1000).await;
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}
