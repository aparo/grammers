// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::sync::Mutex;

use futures_core::future::BoxFuture;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Acquire as _, Row as _};

use crate::types::{
    ChannelKind, ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerKind, UpdateState,
    UpdatesState,
};
use crate::{DEFAULT_DC, KNOWN_DC_OPTIONS, Session};

struct Cache {
    pub home_dc: i32,
    pub dc_options: HashMap<i32, DcOption>,
}

/// PostgreSQL-based session storage.
///
/// Multiple sessions can be hosted in the same database; each is namespaced
/// by the `session_name` provided when the storage is opened.
pub struct PostgresSession {
    pool: PgPool,
    session_name: String,
    cache: Mutex<Cache>,
}

#[repr(u8)]
enum PeerSubtype {
    UserSelf = 1,
    UserBot = 2,
    UserSelfBot = 3,
    Megagroup = 4,
    Broadcast = 8,
    Gigagroup = 12,
}

async fn migrate(pool: &PgPool) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grammers_dc_home (
            session_name TEXT NOT NULL,
            dc_id INTEGER NOT NULL,
            PRIMARY KEY (session_name)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grammers_dc_option (
            session_name TEXT NOT NULL,
            dc_id INTEGER NOT NULL,
            ipv4 TEXT NOT NULL,
            ipv6 TEXT NOT NULL,
            auth_key BYTEA,
            PRIMARY KEY (session_name, dc_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grammers_peer_info (
            session_name TEXT NOT NULL,
            peer_id BIGINT NOT NULL,
            hash BIGINT,
            subtype SMALLINT,
            PRIMARY KEY (session_name, peer_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grammers_update_state (
            session_name TEXT NOT NULL,
            pts INTEGER NOT NULL,
            qts INTEGER NOT NULL,
            date INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            PRIMARY KEY (session_name)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS grammers_channel_state (
            session_name TEXT NOT NULL,
            peer_id BIGINT NOT NULL,
            pts INTEGER NOT NULL,
            PRIMARY KEY (session_name, peer_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn load_cache(pool: &PgPool, session_name: &str) -> sqlx::Result<Cache> {
    let home_dc: i32 = sqlx::query_scalar(
        "SELECT dc_id FROM grammers_dc_home WHERE session_name = $1 LIMIT 1",
    )
    .bind(session_name)
    .fetch_optional(pool)
    .await?
    .unwrap_or(DEFAULT_DC);

    let rows = sqlx::query(
        "SELECT dc_id, ipv4, ipv6, auth_key FROM grammers_dc_option WHERE session_name = $1",
    )
    .bind(session_name)
    .fetch_all(pool)
    .await?;

    let mut dc_options = HashMap::new();
    for row in rows {
        let id: i32 = row.try_get("dc_id")?;
        let ipv4: String = row.try_get("ipv4")?;
        let ipv6: String = row.try_get("ipv6")?;
        let auth_key: Option<Vec<u8>> = row.try_get("auth_key")?;
        let dc_option = DcOption {
            id,
            ipv4: ipv4.parse().unwrap(),
            ipv6: ipv6.parse().unwrap(),
            auth_key: auth_key.map(|k| k.try_into().unwrap()),
        };
        dc_options.insert(dc_option.id, dc_option);
    }

    Ok(Cache {
        home_dc,
        dc_options,
    })
}

impl PostgresSession {
    /// Open a PostgreSQL connection pool for the given `url`,
    /// running the schema migration if necessary.
    ///
    /// `session_name` namespaces all stored data so the same database
    /// can host multiple independent sessions.
    pub async fn connect(url: &str, session_name: impl Into<String>) -> sqlx::Result<Self> {
        let pool = PgPoolOptions::new().connect(url).await?;
        Self::with_pool(pool, session_name).await
    }

    /// Build a session using a caller-provided pool. The schema migration
    /// will be run on the pool if the tables do not exist yet.
    pub async fn with_pool(pool: PgPool, session_name: impl Into<String>) -> sqlx::Result<Self> {
        let session_name = session_name.into();
        migrate(&pool).await?;
        let cache = load_cache(&pool, &session_name).await?;
        Ok(Self {
            pool,
            session_name,
            cache: Mutex::new(cache),
        })
    }

    /// The pool backing this session.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl Session for PostgresSession {
    fn home_dc_id(&self) -> i32 {
        self.cache.lock().unwrap().home_dc
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, ()> {
        self.cache.lock().unwrap().home_dc = dc_id;
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO grammers_dc_home (session_name, dc_id) VALUES ($1, $2)
                 ON CONFLICT (session_name) DO UPDATE SET dc_id = EXCLUDED.dc_id",
            )
            .bind(&self.session_name)
            .bind(dc_id)
            .execute(&self.pool)
            .await
            .unwrap();
        })
    }

    fn dc_option(&self, dc_id: i32) -> Option<DcOption> {
        self.cache
            .lock()
            .unwrap()
            .dc_options
            .get(&dc_id)
            .cloned()
            .or_else(|| {
                KNOWN_DC_OPTIONS
                    .iter()
                    .find(|dc_option| dc_option.id == dc_id)
                    .cloned()
            })
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, ()> {
        self.cache
            .lock()
            .unwrap()
            .dc_options
            .insert(dc_option.id, dc_option.clone());

        let dc_option = dc_option.clone();
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO grammers_dc_option (session_name, dc_id, ipv4, ipv6, auth_key)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (session_name, dc_id) DO UPDATE
                 SET ipv4 = EXCLUDED.ipv4,
                     ipv6 = EXCLUDED.ipv6,
                     auth_key = EXCLUDED.auth_key",
            )
            .bind(&self.session_name)
            .bind(dc_option.id)
            .bind(dc_option.ipv4.to_string())
            .bind(dc_option.ipv6.to_string())
            .bind(dc_option.auth_key.map(|k| k.to_vec()))
            .execute(&self.pool)
            .await
            .unwrap();
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Option<PeerInfo>> {
        Box::pin(async move {
            let row = if let Some(peer_id) = peer.bot_api_dialog_id() {
                sqlx::query(
                    "SELECT peer_id, hash, subtype FROM grammers_peer_info
                     WHERE session_name = $1 AND peer_id = $2 LIMIT 1",
                )
                .bind(&self.session_name)
                .bind(peer_id)
                .fetch_optional(&self.pool)
                .await
                .unwrap()
            } else {
                sqlx::query(
                    "SELECT peer_id, hash, subtype FROM grammers_peer_info
                     WHERE session_name = $1 AND (subtype & $2) <> 0 LIMIT 1",
                )
                .bind(&self.session_name)
                .bind(PeerSubtype::UserSelf as i16)
                .fetch_optional(&self.pool)
                .await
                .unwrap()
            };

            row.map(|row| {
                let raw_subtype: Option<i16> = row.try_get("subtype").unwrap();
                let subtype = raw_subtype.map(|s| s as u8);
                let hash: Option<i64> = row.try_get("hash").unwrap();
                let stored_peer_id: i64 = row.try_get("peer_id").unwrap();
                match peer.kind() {
                    PeerKind::User => PeerInfo::User {
                        id: PeerId::user_unchecked(stored_peer_id).bare_id_unchecked(),
                        auth: hash.map(PeerAuth::from_hash),
                        bot: subtype.map(|s| s & PeerSubtype::UserBot as u8 != 0),
                        is_self: subtype.map(|s| s & PeerSubtype::UserSelf as u8 != 0),
                    },
                    PeerKind::Chat => PeerInfo::Chat {
                        id: peer.bare_id_unchecked(),
                    },
                    PeerKind::Channel => PeerInfo::Channel {
                        id: peer.bare_id_unchecked(),
                        auth: hash.map(PeerAuth::from_hash),
                        kind: subtype.and_then(|s| {
                            if (s & PeerSubtype::Gigagroup as u8) == PeerSubtype::Gigagroup as u8 {
                                Some(ChannelKind::Gigagroup)
                            } else if s & PeerSubtype::Broadcast as u8 != 0 {
                                Some(ChannelKind::Broadcast)
                            } else if s & PeerSubtype::Megagroup as u8 != 0 {
                                Some(ChannelKind::Megagroup)
                            } else {
                                None
                            }
                        }),
                    },
                }
            })
        })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, ()> {
        let peer = peer.clone();
        Box::pin(async move {
            let peer = if let Some(mut existing_peer) = self.peer(peer.id()).await {
                existing_peer.extend_info(&peer);
                existing_peer
            } else {
                peer
            };

            let subtype = match peer {
                PeerInfo::User { bot, is_self, .. } => {
                    match (bot.unwrap_or_default(), is_self.unwrap_or_default()) {
                        (true, true) => Some(PeerSubtype::UserSelfBot),
                        (true, false) => Some(PeerSubtype::UserBot),
                        (false, true) => Some(PeerSubtype::UserSelf),
                        (false, false) => None,
                    }
                }
                PeerInfo::Chat { .. } => None,
                PeerInfo::Channel { kind, .. } => kind.map(|kind| match kind {
                    ChannelKind::Megagroup => PeerSubtype::Megagroup,
                    ChannelKind::Broadcast => PeerSubtype::Broadcast,
                    ChannelKind::Gigagroup => PeerSubtype::Gigagroup,
                }),
            };

            let peer_id = peer.id().bot_api_dialog_id_unchecked();
            let hash = peer.auth().map(|auth| auth.hash());
            let subtype = subtype.map(|s| s as i16);

            sqlx::query(
                "INSERT INTO grammers_peer_info (session_name, peer_id, hash, subtype)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (session_name, peer_id) DO UPDATE
                 SET hash = EXCLUDED.hash,
                     subtype = EXCLUDED.subtype",
            )
            .bind(&self.session_name)
            .bind(peer_id)
            .bind(hash)
            .bind(subtype)
            .execute(&self.pool)
            .await
            .unwrap();
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, UpdatesState> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT pts, qts, date, seq FROM grammers_update_state
                 WHERE session_name = $1 LIMIT 1",
            )
            .bind(&self.session_name)
            .fetch_optional(&self.pool)
            .await
            .unwrap();

            let mut state = if let Some(row) = row {
                UpdatesState {
                    pts: row.try_get("pts").unwrap(),
                    qts: row.try_get("qts").unwrap(),
                    date: row.try_get("date").unwrap(),
                    seq: row.try_get("seq").unwrap(),
                    channels: Vec::new(),
                }
            } else {
                UpdatesState::default()
            };

            let channel_rows = sqlx::query(
                "SELECT peer_id, pts FROM grammers_channel_state WHERE session_name = $1",
            )
            .bind(&self.session_name)
            .fetch_all(&self.pool)
            .await
            .unwrap();

            state.channels = channel_rows
                .into_iter()
                .map(|row| ChannelState {
                    id: row.try_get("peer_id").unwrap(),
                    pts: row.try_get("pts").unwrap(),
                })
                .collect();

            state
        })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut conn = self.pool.acquire().await.unwrap();
            let mut tx = conn.begin().await.unwrap();

            match update {
                UpdateState::All(updates_state) => {
                    sqlx::query("DELETE FROM grammers_update_state WHERE session_name = $1")
                        .bind(&self.session_name)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    sqlx::query(
                        "INSERT INTO grammers_update_state (session_name, pts, qts, date, seq)
                         VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(&self.session_name)
                    .bind(updates_state.pts)
                    .bind(updates_state.qts)
                    .bind(updates_state.date)
                    .bind(updates_state.seq)
                    .execute(&mut *tx)
                    .await
                    .unwrap();

                    sqlx::query("DELETE FROM grammers_channel_state WHERE session_name = $1")
                        .bind(&self.session_name)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    for channel in updates_state.channels {
                        sqlx::query(
                            "INSERT INTO grammers_channel_state (session_name, peer_id, pts)
                             VALUES ($1, $2, $3)",
                        )
                        .bind(&self.session_name)
                        .bind(channel.id)
                        .bind(channel.pts)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    }
                }
                UpdateState::Primary { pts, date, seq } => {
                    sqlx::query(
                        "INSERT INTO grammers_update_state (session_name, pts, qts, date, seq)
                         VALUES ($1, $2, 0, $3, $4)
                         ON CONFLICT (session_name) DO UPDATE
                         SET pts = EXCLUDED.pts,
                             date = EXCLUDED.date,
                             seq = EXCLUDED.seq",
                    )
                    .bind(&self.session_name)
                    .bind(pts)
                    .bind(date)
                    .bind(seq)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
                UpdateState::Secondary { qts } => {
                    sqlx::query(
                        "INSERT INTO grammers_update_state (session_name, pts, qts, date, seq)
                         VALUES ($1, 0, $2, 0, 0)
                         ON CONFLICT (session_name) DO UPDATE SET qts = EXCLUDED.qts",
                    )
                    .bind(&self.session_name)
                    .bind(qts)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
                UpdateState::Channel { id, pts } => {
                    sqlx::query(
                        "INSERT INTO grammers_channel_state (session_name, peer_id, pts)
                         VALUES ($1, $2, $3)
                         ON CONFLICT (session_name, peer_id) DO UPDATE SET pts = EXCLUDED.pts",
                    )
                    .bind(&self.session_name)
                    .bind(id)
                    .bind(pts)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
            }

            tx.commit().await.unwrap();
        })
    }
}
