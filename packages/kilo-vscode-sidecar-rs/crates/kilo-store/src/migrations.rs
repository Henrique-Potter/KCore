//! Versioned, idempotent schema migrations.
//!
//! Per **Storage and process self-healing invariants → 2** of the
//! migration plan: the SQLite schema carries its version in
//! [`PRAGMA user_version`] and a migration runner walks from the on-disk
//! version to the binary version on every writer-connection init.
//!
//! ## Authoring rules
//!
//! 1. Append-only. Never edit, reorder, or delete an entry in
//!    [`MIGRATIONS`] once it has shipped — a user with on-disk version `N`
//!    must always be able to upgrade by replaying versions `N+1..LATEST`
//!    in order. Editing a shipped entry corrupts databases that already
//!    ran it.
//! 2. Each entry runs in its own transaction (the runner wraps the SQL).
//!    Migrations are idempotent — use `add column if not exists`-style
//!    patterns where SQLite supports them, and version-gate `alter table`
//!    elsewhere. The binary's job is to converge state, not to assume
//!    the on-disk schema was produced by the same binary.
//! 3. Adding a column without bumping `LATEST_SCHEMA_VERSION` is a
//!    process bug. CI gate (`m0_schema_version_bumped_when_init_changes`)
//!    fails any change to `init_schema` that does not also append here.
//! 4. Bun-compat reads must keep working until M14 step 7 removes the
//!    Bun fallback. Migrations that change persisted shape MUST land
//!    alongside a new round-trip oracle case.

use rusqlite::Connection;

/// Latest schema version the binary writes. Incremented in lockstep with
/// any change to `init_schema` or any append to [`MIGRATIONS`].
pub(crate) const LATEST_SCHEMA_VERSION: u32 = 3;

/// Entry in the migrations table. `version` is the version this entry
/// brings the database TO (i.e. running entry `2` requires the database
/// to already be at version `1`). `sql` runs inside a transaction so a
/// failing migration leaves the on-disk state at the prior version.
struct Migration {
    version: u32,
    sql: &'static str,
}

/// Append-only list. Entry `i` brings the database from version `i` to
/// version `i+1`. The first entry (version 1) is the M6.1 schema; every
/// subsequent entry is an alteration relative to the previous version.
///
/// IMPORTANT: never edit an entry once it has shipped. Add new entries
/// at the end and bump `LATEST_SCHEMA_VERSION`.
const MIGRATIONS: &[Migration] = &[
    // Version 1: the M6.1 baseline schema. The `init_schema` body in
    // `lib.rs` produces this exact shape via `create table if not exists`.
    // We keep it represented here for completeness so that a fresh DB on
    // a future binary version can also be reconstructed by replaying
    // migrations from zero, even if `init_schema` were ever simplified.
    Migration {
        version: 1,
        sql: "create table if not exists project (
            id text primary key,
            worktree text not null,
            vcs text,
            name text,
            icon_url text,
            icon_url_override text,
            icon_color text,
            time_created integer not null,
            time_updated integer not null,
            time_initialized integer,
            sandboxes text not null,
            commands text
        );
        create table if not exists session (
            id text primary key,
            project_id text not null references project(id) on delete cascade,
            workspace_id text,
            parent_id text,
            slug text not null,
            directory text not null,
            title text not null,
            version text not null,
            share_url text,
            summary_additions integer,
            summary_deletions integer,
            summary_files integer,
            summary_diffs text,
            revert text,
            permission text,
            time_created integer not null,
            time_updated integer not null,
            time_compacting integer,
            time_archived integer
        );
        create table if not exists message (
            id text primary key,
            session_id text not null references session(id) on delete cascade,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );
        create table if not exists part (
            id text primary key,
            message_id text not null references message(id) on delete cascade,
            session_id text not null,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );
        create table if not exists event_sequence (
            aggregate_id text not null primary key,
            seq integer not null
        );
        create table if not exists event (
            id text primary key,
            aggregate_id text not null references event_sequence(aggregate_id) on delete cascade,
            seq integer not null,
            type text not null,
            data text not null
        );",
    },
    // Version 2: Bun-compatible per-session todo state
    // (`packages/opencode/src/session/session.sql.ts::TodoTable`).
    Migration {
        version: 2,
        sql: "create table if not exists todo (
            session_id text not null references session(id) on delete cascade,
            content text not null,
            status text not null,
            priority text not null,
            position integer not null,
            time_created integer not null,
            time_updated integer not null,
            primary key (session_id, position)
        );
        create index if not exists todo_session_idx on todo(session_id);",
    },
    // Version 3: anchor `event_sequence.aggregate_id` to `session(id)` so
    // `delete from session` cascades through `event_sequence` (and from
    // there through `event`). Pre-v3 deletes left orphaned rows because
    // `event_sequence` had no FK back to `session`. `write_event` is only
    // called with a session id (see `lib.rs` call sites near
    // `write_event(&tx, &session.id, ...)`).
    //
    // SQLite cannot add a FK via `alter table`, so we rebuild the two
    // tables. Steps run inside the migration runner's transaction so a
    // failure rolls back cleanly. The temp tables (`event_sequence_new`,
    // `event_new`) are created and renamed within the same migration; if
    // a prior attempt aborted, the rollback discards them.
    //
    // Orphan cleanup must run first: any rows whose `aggregate_id` does
    // not match a current `session.id` would otherwise violate the new
    // FK during the `insert ... select` rebuild. These rows are dead
    // weight from the pre-v3 cascade gap — the session that produced
    // them is already gone.
    Migration {
        version: 3,
        sql: "delete from event where aggregate_id not in (select id from session);
        delete from event_sequence where aggregate_id not in (select id from session);
        create table event_sequence_new (
            aggregate_id text not null primary key references session(id) on delete cascade,
            seq integer not null
        );
        insert into event_sequence_new (aggregate_id, seq)
            select aggregate_id, seq from event_sequence;
        create table event_new (
            id text primary key,
            aggregate_id text not null references event_sequence(aggregate_id) on delete cascade,
            seq integer not null,
            type text not null,
            data text not null
        );
        insert into event_new (id, aggregate_id, seq, type, data)
            select id, aggregate_id, seq, type, data from event;
        drop table event;
        drop table event_sequence;
        alter table event_sequence_new rename to event_sequence;
        alter table event_new rename to event;",
    },
];

/// Read the current `PRAGMA user_version` from the connection. SQLite
/// stores this as a 32-bit integer in the database header; new databases
/// default to 0.
pub(crate) fn current_version(db: &Connection) -> rusqlite::Result<u32> {
    let value: i64 = db.query_row("pragma user_version", [], |row| row.get(0))?;
    Ok(u32::try_from(value.max(0)).unwrap_or(0))
}

/// Walk migrations from `current_version(db)` to [`LATEST_SCHEMA_VERSION`],
/// running each in a transaction. Idempotent: a database already at
/// `LATEST_SCHEMA_VERSION` returns `Ok(())` without executing any SQL.
///
/// Failures leave on-disk state at the highest successfully applied
/// version because each step is its own transaction. The caller is
/// expected to surface the error through [`crate::lib_internal_error`]
/// (post-Phase-1d) so users see a stable `name` instead of raw rusqlite
/// text.
pub(crate) fn run(db: &mut Connection) -> rusqlite::Result<()> {
    let mut current = current_version(db)?;
    for migration in MIGRATIONS {
        if migration.version <= current {
            continue;
        }
        // SQL DDL runs in a transaction so a partial migration rolls back.
        let tx = db.transaction()?;
        tx.execute_batch(migration.sql)?;
        tx.commit()?;
        // `PRAGMA user_version = N` MUST run outside a transaction —
        // SQLite silently ignores it inside `BEGIN/COMMIT` on some
        // versions. Running it after the commit is correct: if the
        // pragma fails, the next launcher will see the prior version
        // and re-run the same migration (idempotent by construction
        // because each migration uses `create table if not exists` and
        // `add column if not exists` patterns).
        //
        // The value is a u32 and originates from a const, so
        // format-injection is safe.
        db.execute_batch(&format!("pragma user_version = {};", migration.version))?;
        current = migration.version;
    }
    if current < LATEST_SCHEMA_VERSION {
        return Err(rusqlite::Error::ExecuteReturnedResults);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn fresh_database_runs_to_latest() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert_eq!(current_version(&conn).unwrap(), 0);
        run(&mut conn).unwrap();
        assert_eq!(current_version(&conn).unwrap(), LATEST_SCHEMA_VERSION);
    }

    #[test]
    fn run_is_idempotent() {
        let mut conn = Connection::open_in_memory().unwrap();
        run(&mut conn).unwrap();
        // Second run is a no-op (current_version == LATEST).
        run(&mut conn).unwrap();
        assert_eq!(current_version(&conn).unwrap(), LATEST_SCHEMA_VERSION);
    }

    #[test]
    fn migration_versions_are_strictly_increasing_from_one() {
        for (idx, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(
                m.version,
                idx as u32 + 1,
                "migration entry {idx} has version {}, expected {}",
                m.version,
                idx + 1
            );
        }
    }

    #[test]
    fn latest_matches_last_entry() {
        let last = MIGRATIONS.last().map(|m| m.version).unwrap_or(0);
        assert_eq!(last, LATEST_SCHEMA_VERSION);
    }
}
