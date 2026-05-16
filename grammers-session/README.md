# grammers-session

This library contains the `Session` trait and several session storages.

Sessions are used to remember the authorization key and home server address,
to prevent having to create a new key and login every single time (which is
an expensive process).

## Storages

The crate ships with three built-in storages, gated by Cargo features:

| Storage           | Type            | Feature flag      | Default |
| ----------------- | --------------- | ----------------- | ------- |
| `MemorySession`   | In-memory       | (always on)       | yes     |
| `SqliteSession`   | SQLite file/DB  | `sqlite-storage`  | yes     |
| `PostgresSession` | PostgreSQL pool | `postgres`        | no      |

You can also implement the `Session` trait yourself to use a different backend.

### PostgreSQL storage

Enable the `postgres` feature to pull in the `sqlx`-based PostgreSQL backend:

```toml
[dependencies]
grammers-session = { version = "0.9", features = ["postgres"] }
```

A single PostgreSQL database can host many independent sessions: every row is
keyed by a `session_name` that you choose when opening the storage. The schema
is created automatically on first open (idempotent, safe to run against a
pre-provisioned database).

```rust,no_run
use grammers_session::storages::PostgresSession;

# async fn open() -> sqlx::Result<()> {
// Open a pool from a connection URL and run the migration.
let session = PostgresSession::connect(
    "postgres://user:pass@localhost/telegram",
    "my-bot",
)
.await?;

// Or reuse a pool you already manage.
let pool = sqlx::postgres::PgPoolOptions::new()
    .connect("postgres://user:pass@localhost/telegram")
    .await?;
let session = PostgresSession::with_pool(pool, "my-bot").await?;
# Ok(()) }
```

The storage creates the following tables, all namespaced by `session_name`:
`grammers_dc_home`, `grammers_dc_option`, `grammers_peer_info`,
`grammers_update_state`, `grammers_channel_state`.
