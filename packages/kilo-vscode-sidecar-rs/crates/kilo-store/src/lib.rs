use std::{
    collections::BTreeMap,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{atomic, Arc, Mutex},
    time::SystemTime,
};

pub mod keyring;
mod migrations;
pub mod paths;

use chrono::{DateTime, Utc};
use kilo_protocol::{
    Config, KiloPath, Message, MessageAppendInput, MessageAppendResult, Project, ProjectCommands,
    ProjectIcon, Session, SessionCreateInput, SessionForkInput, SessionRevertInput, SessionTime,
    SessionUpdateInput, Time,
};
use rand::{rngs::OsRng, RngCore};
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection, OpenFlags, OptionalExtension,
    Row,
};
use serde_json::{json, Map, Value as JsonValue};

static IDS: atomic::AtomicU64 = atomic::AtomicU64::new(0);
static AUTH_WRITES: atomic::AtomicU64 = atomic::AtomicU64::new(0);

#[derive(Clone)]
pub struct Store {
    paths: Paths,
    directory: String,
    worktree: String,
    /// Audit Fix 9: cached writer connection. Lazily initialized on first
    /// write, then reused. Pragmas (`journal_mode=WAL`, `foreign_keys=ON`,
    /// `busy_timeout=5000`) are stamped on first open. Wrapped in
    /// `Arc<Mutex<Option<…>>>` so `Store: Clone + Send + Sync` and so we
    /// can swap it out atomically when `for_test` builds a fresh store.
    writer: Arc<Mutex<Option<Connection>>>,
    /// Item 11 perf: cached read-only connection, peer to `writer`.
    /// Lazily initialized on first `with_db` call. Held under the same
    /// `Arc<Mutex<Option<…>>>` shape so `Store: Clone + Send + Sync`.
    /// SQLite WAL allows concurrent reads without blocking writes, so a
    /// long-lived reader is safe.
    reader: Arc<Mutex<Option<Connection>>>,
}

#[derive(Clone)]
struct Paths {
    home: PathBuf,
    data: PathBuf,
    config: PathBuf,
    state: PathBuf,
}

#[derive(Default)]
pub struct SessionQuery {
    pub directory: Option<String>,
    pub roots: bool,
    pub start: Option<i64>,
    pub search: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct MessageCursor {
    pub id: String,
    pub time: i64,
}

#[derive(Clone, Debug)]
pub struct MessagePage {
    pub items: Vec<Message>,
    pub cursor: Option<MessageCursor>,
    pub more: bool,
}

struct MessageRow {
    message: Message,
    cursor: MessageCursor,
}

#[derive(Clone, Debug)]
pub struct StoredEvent {
    pub id: String,
    pub seq: i64,
    pub aggregate_id: String,
    pub data: JsonValue,
    pub event_type: String,
}

#[derive(Clone, Debug)]
pub struct AppendRecord {
    pub result: MessageAppendResult,
    pub events: Vec<StoredEvent>,
}

#[derive(Clone, Debug)]
pub struct SessionRecord {
    pub session: Session,
    pub event: StoredEvent,
}

#[derive(Clone, Debug)]
pub struct DeleteRecord {
    pub session: Option<Session>,
    pub event: Option<StoredEvent>,
}

#[derive(Clone, Debug)]
pub struct SessionMutation {
    pub session: Option<Session>,
    pub events: Vec<StoredEvent>,
}

#[derive(Clone, Debug)]
pub struct ForkRecord {
    pub session: Session,
    pub events: Vec<StoredEvent>,
}

#[derive(Clone, Debug)]
pub struct MessageRecord {
    pub message: Option<Message>,
    pub events: Vec<StoredEvent>,
}

#[derive(Clone, Debug)]
pub struct PartRecord {
    pub part: Option<JsonValue>,
    pub events: Vec<StoredEvent>,
}

impl Store {
    pub fn new() -> Self {
        let paths = Paths::new();
        let directory = env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .to_string_lossy()
            .to_string();

        Self {
            paths,
            worktree: directory.clone(),
            directory,
            writer: Arc::new(Mutex::new(None)),
            reader: Arc::new(Mutex::new(None)),
        }
    }

    /// Resolve the state directory without constructing a full `Store`.
    /// Used at startup by the binary's tracing-subscriber init so the
    /// log-file path doesn't pay the price of allocating a writer-mutex
    /// + reading `current_dir()`. Callers that already hold a `Store`
    /// should prefer [`Store::paths`] instead.
    pub fn resolve_state_dir() -> PathBuf {
        Paths::new().state
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn for_test(root: &std::path::Path) -> Self {
        Self {
            paths: Paths {
                home: root.join("home"),
                data: root.join("data").join("kilo"),
                config: root.join("config").join("kilo"),
                state: root.join("state").join("kilo"),
            },
            directory: root.join("repo").to_string_lossy().to_string(),
            worktree: root.join("repo").to_string_lossy().to_string(),
            writer: Arc::new(Mutex::new(None)),
            reader: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)]
    pub fn seed_for_test(&self) {
        fs::create_dir_all(&self.paths.data).unwrap();
        let db = Connection::open(self.paths.data.join("kilo.db")).unwrap();
        db.execute_batch(
            "create table project (
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
            create table session (
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
            create table message (
                id text primary key,
                session_id text not null references session(id) on delete cascade,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );
            create table part (
                id text primary key,
                message_id text not null references message(id) on delete cascade,
                session_id text not null,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );
            create table event_sequence (
                aggregate_id text not null primary key,
                seq integer not null
            );
            create table event (
                id text primary key,
                aggregate_id text not null references event_sequence(aggregate_id) on delete cascade,
                seq integer not null,
                type text not null,
                data text not null
            );",
        )
        .unwrap();
    }

    pub fn paths(&self) -> KiloPath {
        KiloPath {
            home: self.paths.home.to_string_lossy().to_string(),
            state: self.paths.state.to_string_lossy().to_string(),
            config: self.paths.config.to_string_lossy().to_string(),
            worktree: self.worktree.clone(),
            directory: self.directory.clone(),
        }
    }

    pub fn config(&self) -> Config {
        read_config(&self.paths.config)
    }

    /// Persist a merged config to whichever file `read_config` would have
    /// chosen on the next read. Mirrors Bun's `globalConfigFile()` priority
    /// in `packages/opencode/src/config/config.ts:345` — pick the highest-
    /// priority existing file (`kilo.json` first, then `opencode.json`,
    /// then `config.json`); if none exist, default to `kilo.json`. Without
    /// this priority match, a user with `opencode.json` would PATCH and
    /// then read back the pre-patch values because `read_config` returns
    /// the highest-priority file unmerged. Fails best-effort: if the
    /// directory cannot be created or the file cannot be written, return
    /// the I/O error so the route can surface a 500.
    pub fn set_config(&self, value: Config) -> std::io::Result<Config> {
        fs::create_dir_all(&self.paths.config)?;
        let target = config_priority()
            .iter()
            .map(|name| self.paths.config.join(name))
            .find(|path| path.exists())
            .unwrap_or_else(|| self.paths.config.join(config_priority()[0]));
        let body = serde_json::to_string_pretty(&value)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        fs::write(&target, body)?;
        Ok(read_config(&self.paths.config))
    }

    pub fn provider_auths(&self) -> BTreeMap<String, JsonValue> {
        read_auths(&self.paths.data)
    }

    pub fn provider_auth(&self, id: &str) -> Option<JsonValue> {
        self.provider_auths().remove(id)
    }

    pub fn set_provider_auth(&self, id: &str, value: JsonValue) -> std::io::Result<JsonValue> {
        fs::create_dir_all(&self.paths.data)?;
        let key = id.trim_end_matches('/');
        let mut data = self.provider_auths();
        data.remove(id);
        data.remove(&format!("{key}/"));
        data.insert(key.to_string(), value.clone());
        write_auths(&self.paths.data, &data)?;
        Ok(value)
    }

    pub fn clear_provider_auth(&self, id: &str) -> std::io::Result<()> {
        fs::create_dir_all(&self.paths.data)?;
        let key = id.trim_end_matches('/');
        let mut data = self.provider_auths();
        data.remove(id);
        data.remove(key);
        data.remove(&format!("{key}/"));
        write_auths(&self.paths.data, &data)
    }

    pub fn mcp_auths(&self) -> BTreeMap<String, JsonValue> {
        read_mcp_auths(&self.paths.data)
    }

    /// Persisted "always" permission rules — Bun parity for
    /// `PermissionTable` (`packages/opencode/src/permission/index.ts:195-201`).
    /// Returns rules in the order they were appended; consumers should
    /// scan with `findLast` / reverse-iter semantics to honor "later
    /// rule wins". Values are opaque JSON (caller types them).
    pub fn permission_rules(&self) -> Vec<JsonValue> {
        read_permission_rules(&self.paths.data)
    }

    /// Append one or more rules to the persisted set. Each call is
    /// strictly additive; nothing here de-dupes (Bun doesn't either) so
    /// the caller is expected to keep the count bounded by the natural
    /// rule namespace.
    pub fn append_permission_rules(&self, rules: &[JsonValue]) -> std::io::Result<()> {
        if rules.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.paths.data)?;
        let mut existing = read_permission_rules(&self.paths.data);
        existing.extend(rules.iter().cloned());
        write_permission_rules(&self.paths.data, &existing)
    }

    /// Test/admin helper — replace the entire persisted rule set. Not
    /// used by the agent loop; exposed so callers (e.g. a future
    /// `/permission/clear` route) can reset.
    pub fn replace_permission_rules(&self, rules: &[JsonValue]) -> std::io::Result<()> {
        fs::create_dir_all(&self.paths.data)?;
        write_permission_rules(&self.paths.data, rules)
    }

    pub fn mcp_auth(&self, id: &str) -> Option<JsonValue> {
        self.mcp_auths().remove(id)
    }

    pub fn set_mcp_auth(&self, id: &str, value: JsonValue) -> std::io::Result<JsonValue> {
        fs::create_dir_all(&self.paths.data)?;
        let mut data = self.mcp_auths();
        data.insert(id.to_string(), value.clone());
        write_mcp_auths(&self.paths.data, &data)?;
        Ok(value)
    }

    /// Resolve the current project for `Instance.project` parity with
    /// `packages/opencode/src/server/routes/instance/index.ts:project/current`.
    ///
    /// Tries the `project` table by worktree first. If the row is missing
    /// (e.g. fresh `.kilo` directory or read-only fallback), returns a
    /// best-effort placeholder that preserves the worktree string so the
    /// sidebar's `git-status.ts` and Agent Manager can still render. The
    /// placeholder mirrors what Bun would have produced before
    /// `Project.fromDirectory` ran.
    pub fn project(&self) -> Project {
        let from_db = self.with_db(|db| read_project_by_worktree(db, &self.worktree));
        if let Some(Some(project)) = from_db {
            return project;
        }
        Project {
            id: "global".to_string(),
            worktree: self.worktree.clone(),
            vcs: None,
            name: None,
            icon: None,
            commands: None,
            time: Time {
                created: 0,
                updated: 0,
                initialized: None,
            },
            sandboxes: vec![],
        }
    }

    pub fn import_project(&self, input: JsonValue) -> rusqlite::Result<String> {
        self.with_write(|db| {
            let data = ensure_map(input)?;
            let id = required_str(&data, "id")?;
            let worktree = required_str(&data, "worktree")?;
            if worktree.trim().is_empty() {
                return Err(rusqlite::Error::InvalidParameterName(
                    "project worktree is required".to_string(),
                ));
            }
            let time = now_millis();
            let created = data
                .get("timeCreated")
                .and_then(JsonValue::as_i64)
                .unwrap_or(time);
            let updated = data
                .get("timeUpdated")
                .and_then(JsonValue::as_i64)
                .unwrap_or(created);
            let sandboxes = data
                .get("sandboxes")
                .cloned()
                .unwrap_or_else(|| JsonValue::Array(Vec::new()));
            let commands = data.get("commands").cloned();
            let tx = db.transaction()?;
            ensure_table(&tx, "project")?;
            tx.execute(
                "insert into project (id, worktree, vcs, name, icon_url, icon_color, time_created, \
                 time_updated, time_initialized, sandboxes, commands) values \
                 (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
                 on conflict(id) do update set worktree = excluded.worktree, vcs = excluded.vcs, \
                 name = excluded.name, icon_url = excluded.icon_url, icon_color = excluded.icon_color, \
                 time_created = excluded.time_created, time_updated = excluded.time_updated, \
                 time_initialized = excluded.time_initialized, sandboxes = excluded.sandboxes, \
                 commands = excluded.commands",
                params![
                    &id,
                    &worktree,
                    opt_str(&data, "vcs"),
                    opt_str(&data, "name"),
                    opt_str(&data, "iconUrl"),
                    opt_str(&data, "iconColor"),
                    created,
                    updated,
                    data.get("timeInitialized").and_then(JsonValue::as_i64),
                    json_value(&sandboxes)?,
                    json_text(&commands),
                ],
            )?;
            tx.commit()?;
            Ok(id)
        })
    }

    pub fn import_session(&self, input: JsonValue) -> rusqlite::Result<(String, bool)> {
        self.with_write(|db| {
            let data = ensure_map(input)?;
            let id = required_str(&data, "id")?;
            let force = data.get("force").and_then(JsonValue::as_bool).unwrap_or(false);
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            let exists = read_session(&tx, &id).is_some();
            if exists && !force {
                tx.commit()?;
                return Ok((id, true));
            }
            if exists {
                tx.execute("delete from session where id = ?1", [&id])?;
            }

            let summary = summary_fields(data.get("summary"));
            tx.execute(
                "insert into session (id, project_id, workspace_id, parent_id, slug, directory, title, version, \
                 share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, \
                 permission, time_created, time_updated, time_compacting, time_archived) \
                 values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19) \
                 on conflict(id) do update set project_id = excluded.project_id, workspace_id = excluded.workspace_id, \
                 parent_id = excluded.parent_id, slug = excluded.slug, directory = excluded.directory, \
                 title = excluded.title, version = excluded.version, share_url = excluded.share_url, \
                 summary_additions = excluded.summary_additions, summary_deletions = excluded.summary_deletions, \
                 summary_files = excluded.summary_files, summary_diffs = excluded.summary_diffs, \
                 revert = excluded.revert, permission = excluded.permission, time_created = excluded.time_created, \
                 time_updated = excluded.time_updated, time_compacting = excluded.time_compacting, \
                 time_archived = excluded.time_archived",
                params![
                    &id,
                    required_str(&data, "projectID")?,
                    opt_str(&data, "workspaceID"),
                    opt_str(&data, "parentID"),
                    required_str(&data, "slug")?,
                    required_str(&data, "directory")?,
                    required_str(&data, "title")?,
                    required_str(&data, "version")?,
                    opt_str(&data, "shareURL"),
                    summary.additions,
                    summary.deletions,
                    summary.files,
                    summary.diffs,
                    json_text(&data.get("revert").cloned()),
                    json_text(&data.get("permission").cloned()),
                    required_i64(&data, "timeCreated")?,
                    required_i64(&data, "timeUpdated")?,
                    data.get("timeCompacting").and_then(JsonValue::as_i64),
                    data.get("timeArchived").and_then(JsonValue::as_i64),
                ],
            )?;
            tx.commit()?;
            Ok((id, false))
        })
    }

    pub fn import_message(&self, input: JsonValue) -> rusqlite::Result<String> {
        self.with_write(|db| {
            let data = ensure_map(input)?;
            let id = required_str(&data, "id")?;
            let time = required_i64(&data, "timeCreated")?;
            let body = data.get("data").cloned().unwrap_or_else(|| json!({}));
            let tx = db.transaction()?;
            ensure_table(&tx, "message")?;
            tx.execute(
                "insert into message (id, session_id, time_created, time_updated, data) values (?1, ?2, ?3, ?4, ?5) \
                 on conflict(id) do update set session_id = excluded.session_id, time_updated = excluded.time_updated, data = excluded.data",
                params![
                    &id,
                    required_str(&data, "sessionID")?,
                    time,
                    time,
                    json_value(&body)?,
                ],
            )?;
            tx.commit()?;
            Ok(id)
        })
    }

    pub fn import_part(&self, input: JsonValue) -> rusqlite::Result<String> {
        self.with_write(|db| {
            let data = ensure_map(input)?;
            let id = required_str(&data, "id")?;
            let time = data
                .get("timeCreated")
                .and_then(JsonValue::as_i64)
                .unwrap_or_else(now_millis);
            let body = data.get("data").cloned().unwrap_or_else(|| json!({}));
            let tx = db.transaction()?;
            ensure_table(&tx, "part")?;
            tx.execute(
                "insert into part (id, message_id, session_id, time_created, time_updated, data) values (?1, ?2, ?3, ?4, ?5, ?6) \
                 on conflict(id) do update set message_id = excluded.message_id, session_id = excluded.session_id, time_updated = excluded.time_updated, data = excluded.data",
                params![
                    &id,
                    required_str(&data, "messageID")?,
                    required_str(&data, "sessionID")?,
                    time,
                    time,
                    json_value(&body)?,
                ],
            )?;
            tx.commit()?;
            Ok(id)
        })
    }

    pub fn sessions(&self, query: &SessionQuery) -> Vec<Session> {
        self.with_db(|db| read_sessions(db, query))
            .unwrap_or_default()
    }

    /// Count sessions in the workspace, optionally filtered by directory.
    /// Used by the session-quota check on `POST /session`. Distinct from
    /// `sessions(...)` because the read path caps results at 500; this
    /// accessor returns the true row count.
    pub fn session_count(&self, directory: Option<&str>) -> usize {
        self.with_db(|db| count_sessions(db, directory))
            .unwrap_or(0)
    }

    pub fn session(&self, id: &str) -> Option<Session> {
        self.with_db(|db| read_session(db, id)).unwrap_or(None)
    }

    pub fn children(&self, id: &str) -> Option<Vec<Session>> {
        self.with_db(|db| read_children(db, id, &self.worktree))
            .unwrap_or(None)
    }

    pub fn create_session(&self, input: SessionCreateInput) -> rusqlite::Result<Session> {
        self.create_session_record(input)
            .map(|record| record.session)
    }

    pub fn create_session_record(
        &self,
        input: SessionCreateInput,
    ) -> rusqlite::Result<SessionRecord> {
        self.with_write(|db| {
            let time = now_millis();
            let tx = db.transaction()?;
            ensure_table(&tx, "project")?;
            ensure_table(&tx, "session")?;
            let project = ensure_project(&tx, &self.worktree, time)?;
            let id = session_id(time);
            let title = input.title.unwrap_or_else(|| default_title(time));
            let session = Session {
                id: id.clone(),
                project_id: project.id,
                workspace_id: input.workspace_id,
                parent_id: input.parent_id,
                slug: slug(&id),
                directory: self.directory.clone(),
                title,
                version: env!("CARGO_PKG_VERSION").to_string(),
                summary: None,
                share: None,
                revert: None,
                permission: input.permission,
                time: SessionTime {
                    created: time,
                    updated: time,
                    compacting: None,
                    archived: None,
                },
            };
            tx.execute(
                "insert into session (id, project_id, workspace_id, parent_id, slug, directory, title, version, \
                 share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, \
                 permission, time_created, time_updated, time_compacting, time_archived) \
                 values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, null, null, null, null, null, null, ?9, ?10, ?11, null, null)",
                params![
                    &session.id,
                    &session.project_id,
                    &session.workspace_id,
                    &session.parent_id,
                    &session.slug,
                    &session.directory,
                    &session.title,
                    &session.version,
                    json_text(&session.permission),
                    session.time.created,
                    session.time.updated,
                ],
            )?;
            let event = write_event(
                &tx,
                &session.id,
                "session.created.v1",
                json!({ "sessionID": session.id, "info": session.clone() }),
            )?;
            tx.commit()?;
            let session = read_session(db, &id).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            Ok(SessionRecord { session, event })
        })
    }

    /// PATCH `/session/{id}`. Bun's session.update accepts `title`,
    /// `permission`, and `time.archived`; absent fields are no-ops so an empty
    /// PATCH returns the current session without touching the row.
    pub fn update_session(
        &self,
        id: &str,
        input: SessionUpdateInput,
    ) -> rusqlite::Result<Option<Session>> {
        let permission = input.permission.and_then(non_null);
        let archived = archived_time(input.time.as_ref());
        if input.title.is_none() && permission.is_none() && archived.is_none() {
            return Ok(self.session(id));
        }
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            let existing = read_session(&tx, id);
            if existing.is_none() {
                tx.commit()?;
                return Ok(None);
            }
            let time = now_millis();
            if let Some(value) = input.title.as_deref() {
                tx.execute(
                    "update session set title = ?1, time_updated = ?2 where id = ?3",
                    params![value, time, id],
                )?;
            }
            if permission.is_some() || archived.is_some() {
                tx.execute(
                    "update session set permission = coalesce(?1, permission), time_archived = coalesce(?2, time_archived), time_updated = ?3 where id = ?4",
                    params![json_text(&permission), archived, time, id],
                )?;
            }
            tx.commit()?;
            Ok(read_session(db, id))
        })
    }

    pub fn delete_session(&self, id: &str) -> rusqlite::Result<Option<Session>> {
        self.delete_session_record(id).map(|record| record.session)
    }

    pub fn delete_session_record(&self, id: &str) -> rusqlite::Result<DeleteRecord> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            let session = read_session(&tx, id);
            let mut event = None;
            if session.is_some() {
                tx.execute("delete from session where id = ?1", [id])?;
                if let Some(session) = &session {
                    event = Some(write_event(
                        &tx,
                        id,
                        "session.deleted.v1",
                        json!({ "sessionID": id, "info": session }),
                    )?);
                }
            }
            tx.commit()?;
            Ok(DeleteRecord { session, event })
        })
    }

    pub fn fork_session_record(
        &self,
        id: &str,
        input: SessionForkInput,
    ) -> rusqlite::Result<Option<ForkRecord>> {
        self.with_write(|db| {
            let time = now_millis();
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            ensure_table(&tx, "project")?;
            ensure_table(&tx, "message")?;
            ensure_table(&tx, "part")?;
            let Some(original) = read_session(&tx, id) else {
                tx.commit()?;
                return Ok(None);
            };
            let project = ensure_project(&tx, &self.worktree, time)?;
            let sid = session_id(time);
            let session = Session {
                id: sid.clone(),
                project_id: project.id,
                workspace_id: original.workspace_id.clone(),
                parent_id: Some(original.id.clone()),
                slug: slug(&sid),
                directory: self.directory.clone(),
                title: fork_title(&original.title),
                version: env!("CARGO_PKG_VERSION").to_string(),
                summary: None,
                share: None,
                revert: None,
                permission: original.permission.clone(),
                time: SessionTime {
                    created: time,
                    updated: time,
                    compacting: None,
                    archived: None,
                },
            };
            insert_session(&tx, &session)?;

            let mut events = vec![write_event(
                &tx,
                &session.id,
                "session.created.v1",
                json!({ "sessionID": session.id, "info": session.clone() }),
            )?];
            let msgs = read_messages(&tx, id, None, None)
                .map(|page| page.items)
                .unwrap_or_default();
            let mut map = BTreeMap::new();
            for msg in msgs {
                let old = id_field(&msg.info, "id").unwrap_or_default();
                if input.message_id.as_deref().is_some_and(|cut| old == cut) {
                    break;
                }
                let mid = message_id(time);
                map.insert(old.clone(), mid.clone());
                let JsonValue::Object(mut info) = ensure_object(msg.info)? else {
                    unreachable!()
                };
                info.insert("id".to_string(), JsonValue::String(mid.clone()));
                info.insert("sessionID".to_string(), JsonValue::String(session.id.clone()));
                if info.get("role").and_then(JsonValue::as_str) == Some("assistant") {
                    if let Some(parent) = info
                        .get("parentID")
                        .and_then(JsonValue::as_str)
                        .and_then(|value| map.get(value))
                        .cloned()
                    {
                        info.insert("parentID".to_string(), JsonValue::String(parent));
                    }
                }
                let info = JsonValue::Object(info);
                let data = data_without(info.clone(), ["id", "sessionID"]);
                tx.execute(
                    "insert into message (id, session_id, time_created, time_updated, data) values (?1, ?2, ?3, ?4, ?5)",
                    params![&mid, &session.id, time, time, json_value(&data)?],
                )?;
                events.push(write_event(
                    &tx,
                    &session.id,
                    "message.updated.v1",
                    json!({ "sessionID": session.id, "info": info }),
                )?);
                for part in msg.parts {
                    let JsonValue::Object(mut part) = ensure_object(part)? else {
                        unreachable!()
                    };
                    part.insert("id".to_string(), JsonValue::String(part_id(time)));
                    part.insert("messageID".to_string(), JsonValue::String(mid.clone()));
                    part.insert("sessionID".to_string(), JsonValue::String(session.id.clone()));
                    let part = write_part(&tx, &session.id, &mid, JsonValue::Object(part), time)?;
                    events.push(write_event(
                        &tx,
                        &session.id,
                        "message.part.updated.v1",
                        json!({ "sessionID": session.id, "part": part, "time": time }),
                    )?);
                }
            }
            tx.commit()?;
            let session = read_session(db, &sid).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            Ok(Some(ForkRecord { session, events }))
        })
    }

    pub fn messages(
        &self,
        id: &str,
        limit: Option<usize>,
        before: Option<&MessageCursor>,
    ) -> Option<MessagePage> {
        self.with_db(|db| read_messages(db, id, limit, before))
            .unwrap_or(None)
    }

    pub fn message(&self, id: &str, mid: &str) -> Option<Message> {
        self.with_db(|db| read_message(db, id, mid)).unwrap_or(None)
    }

    pub fn append_message(
        &self,
        id: &str,
        input: MessageAppendInput,
    ) -> rusqlite::Result<MessageAppendResult> {
        self.append_message_record(id, input)
            .map(|record| record.result)
    }

    pub fn append_message_record(
        &self,
        id: &str,
        input: MessageAppendInput,
    ) -> rusqlite::Result<AppendRecord> {
        self.with_write(|db| {
            let time = now_millis();
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            ensure_table(&tx, "message")?;
            ensure_table(&tx, "part")?;

            read_session(&tx, id).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            let info = info_with_session(input.info, id, time)?;
            let mid = id_field(&info, "id").unwrap_or_else(|| message_id(time));
            let data = data_without(info.clone(), ["id", "sessionID"]);
            tx.execute(
                "insert into message (id, session_id, time_created, time_updated, data) values (?1, ?2, ?3, ?4, ?5) \
                 on conflict(id) do update set session_id = excluded.session_id, time_updated = excluded.time_updated, data = excluded.data",
                params![&mid, id, time, time, json_value(&data)?],
            )?;

            let parts = input
                .parts
                .into_iter()
                .map(|part| write_part(&tx, id, &mid, part, time))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            tx.execute("update session set time_updated = ?1 where id = ?2", params![time, id])?;

            let mut info = info;
            info["id"] = JsonValue::String(mid.clone());
            info["sessionID"] = JsonValue::String(id.to_string());
            let mut events = Vec::new();
            events.push(write_event(
                &tx,
                id,
                "message.updated.v1",
                json!({ "sessionID": id, "info": info.clone() }),
            )?);
            for part in &parts {
                events.push(write_event(
                    &tx,
                    id,
                    "message.part.updated.v1",
                    json!({ "sessionID": id, "part": part, "time": time }),
                )?);
            }
            tx.commit()?;

            Ok(AppendRecord {
                result: MessageAppendResult { info, parts, time },
                events,
            })
        })
    }

    pub fn remove_message_record(&self, id: &str, mid: &str) -> rusqlite::Result<MessageRecord> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            ensure_table(&tx, "message")?;
            ensure_table(&tx, "part")?;
            read_session(&tx, id).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            let message = read_message(&tx, id, mid);
            let mut events = Vec::new();
            if message.is_some() {
                tx.execute(
                    "delete from message where session_id = ?1 and id = ?2",
                    params![id, mid],
                )?;
                events.push(write_event(
                    &tx,
                    id,
                    "message.removed.v1",
                    json!({ "sessionID": id, "messageID": mid }),
                )?);
            }
            tx.commit()?;
            Ok(MessageRecord { message, events })
        })
    }

    pub fn remove_part_record(
        &self,
        id: &str,
        mid: &str,
        pid: &str,
    ) -> rusqlite::Result<PartRecord> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            ensure_table(&tx, "message")?;
            ensure_table(&tx, "part")?;
            read_message(&tx, id, mid).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            let part = read_part(&tx, id, mid, pid);
            let mut events = Vec::new();
            if part.is_some() {
                tx.execute(
                    "delete from part where session_id = ?1 and message_id = ?2 and id = ?3",
                    params![id, mid, pid],
                )?;
                events.push(write_event(
                    &tx,
                    id,
                    "message.part.removed.v1",
                    json!({ "sessionID": id, "messageID": mid, "partID": pid }),
                )?);
            }
            tx.commit()?;
            Ok(PartRecord { part, events })
        })
    }

    pub fn update_part_record(
        &self,
        id: &str,
        mid: &str,
        pid: &str,
        part: JsonValue,
    ) -> rusqlite::Result<PartRecord> {
        self.with_write(|db| {
            let time = now_millis();
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            ensure_table(&tx, "message")?;
            ensure_table(&tx, "part")?;
            read_message(&tx, id, mid).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            if id_field(&part, "id").is_some_and(|value| value != pid)
                || id_field(&part, "messageID").is_some_and(|value| value != mid)
                || id_field(&part, "sessionID").is_some_and(|value| value != id)
            {
                return Err(rusqlite::Error::InvalidParameterName(
                    "part path/body id mismatch".to_string(),
                ));
            }
            let part = write_part(&tx, id, mid, part, time)?;
            let event = write_event(
                &tx,
                id,
                "message.part.updated.v1",
                json!({ "sessionID": id, "part": part.clone(), "time": time }),
            )?;
            tx.commit()?;
            Ok(PartRecord {
                part: Some(part),
                events: vec![event],
            })
        })
    }

    pub fn set_revert_record(
        &self,
        id: &str,
        input: SessionRevertInput,
    ) -> rusqlite::Result<SessionMutation> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            let Some(existing) = read_session(&tx, id) else {
                tx.commit()?;
                return Ok(SessionMutation {
                    session: None,
                    events: Vec::new(),
                });
            };
            let time = now_millis();
            let revert = input.revert.or_else(|| {
                input.message_id.as_ref().map(|mid| {
                    let mut data = json!({ "messageID": mid });
                    if let Some(pid) = input.part_id {
                        data["partID"] = JsonValue::String(pid);
                    }
                    data
                })
            });
            if revert.is_none() && input.summary.is_none() {
                tx.commit()?;
                return Ok(SessionMutation {
                    session: Some(existing),
                    events: Vec::new(),
                });
            }
            let summary = summary_fields(input.summary.as_ref());
            tx.execute(
                "update session set revert = coalesce(?1, revert), summary_additions = coalesce(?2, summary_additions), \
                 summary_deletions = coalesce(?3, summary_deletions), summary_files = coalesce(?4, summary_files), \
                 summary_diffs = coalesce(?5, summary_diffs), time_updated = ?6 where id = ?7",
                params![
                    json_text(&revert),
                    summary.additions,
                    summary.deletions,
                    summary.files,
                    summary.diffs,
                    time,
                    id,
                ],
            )?;
            let event = write_event(
                &tx,
                id,
                "session.updated.v1",
                json!({ "sessionID": id, "info": { "revert": revert, "summary": input.summary, "time": { "updated": time } } }),
            )?;
            tx.commit()?;
            Ok(SessionMutation {
                session: read_session(db, id),
                events: vec![event],
            })
        })
    }

    pub fn clear_revert_record(&self, id: &str) -> rusqlite::Result<SessionMutation> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            if read_session(&tx, id).is_none() {
                tx.commit()?;
                return Ok(SessionMutation {
                    session: None,
                    events: Vec::new(),
                });
            }
            let time = now_millis();
            tx.execute(
                "update session set revert = null, time_updated = ?1 where id = ?2",
                params![time, id],
            )?;
            let event = write_event(
                &tx,
                id,
                "session.updated.v1",
                json!({ "sessionID": id, "info": { "revert": null, "time": { "updated": time } } }),
            )?;
            tx.commit()?;
            Ok(SessionMutation {
                session: read_session(db, id),
                events: vec![event],
            })
        })
    }

    pub fn diff(&self, id: &str) -> Option<Vec<JsonValue>> {
        self.session(id)?;
        let stored = self
            .session(id)
            .and_then(|session| session.summary)
            .and_then(|summary| summary.get("diffs").cloned())
            .and_then(|diffs| diffs.as_array().cloned())
            .unwrap_or_default();
        if !stored.is_empty() {
            return Some(stored);
        }

        Some(
            self.with_db(|db| session_tool_diffs(db, id))
                .unwrap_or_default(),
        )
    }

    pub fn set_share_record(
        &self,
        id: &str,
        url: Option<String>,
    ) -> rusqlite::Result<SessionMutation> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            if read_session(&tx, id).is_none() {
                tx.commit()?;
                return Ok(SessionMutation { session: None, events: Vec::new() });
            }
            let time = now_millis();
            let url = url.unwrap_or_else(|| format!("kilo://share/{id}"));
            tx.execute(
                "update session set share_url = ?1, time_updated = ?2 where id = ?3",
                params![&url, time, id],
            )?;
            let event = write_event(
                &tx,
                id,
                "session.updated.v1",
                json!({ "sessionID": id, "info": { "share": { "url": url }, "time": { "updated": time } } }),
            )?;
            tx.commit()?;
            Ok(SessionMutation { session: read_session(db, id), events: vec![event] })
        })
    }

    pub fn clear_share_record(&self, id: &str) -> rusqlite::Result<SessionMutation> {
        self.with_write(|db| {
            let tx = db.transaction()?;
            ensure_table(&tx, "session")?;
            if read_session(&tx, id).is_none() {
                tx.commit()?;
                return Ok(SessionMutation { session: None, events: Vec::new() });
            }
            let time = now_millis();
            tx.execute(
                "update session set share_url = null, time_updated = ?1 where id = ?2",
                params![time, id],
            )?;
            let event = write_event(
                &tx,
                id,
                "session.updated.v1",
                json!({ "sessionID": id, "info": { "share": { "url": null }, "time": { "updated": time } } }),
            )?;
            tx.commit()?;
            Ok(SessionMutation { session: read_session(db, id), events: vec![event] })
        })
    }

    fn with_db<T>(&self, f: impl FnOnce(&Connection) -> T) -> Option<T> {
        // Pre-cache: every read opened a fresh `Connection`, ran the
        // SQLite open syscall + URI parse, then dropped it. With ~10
        // session/message reads per turn (sidebar refresh + agent
        // manager + history) and several turns per minute, the open/close
        // churn shows up in flamegraphs.
        //
        // Cache: a single read-only connection per `Store` clone, lazily
        // initialized on first read, guarded by a `Mutex` for cross-thread
        // safety. SQLite is opened with `SQLITE_OPEN_NO_MUTEX` so the
        // outer `Mutex` is the only synchronization point — same
        // discipline as the writer cache.
        //
        // The cached reader holds an open file descriptor. SQLite's WAL
        // mode is reader-friendly: writers don't block readers and vice
        // versa, so a long-lived reader is harmless. If the file is
        // deleted (rare; tempdir cleanup), the next access fails the
        // `path.exists()` precheck and we fall back to opening fresh.
        let path = self.paths.data.join("kilo.db");
        if !path.exists() {
            // Drop any cached reader so a re-created database (test
            // teardown / fresh-install) gets a new connection.
            if let Ok(mut guard) = self.reader.lock() {
                *guard = None;
            }
            return None;
        }

        let mut guard = match self.reader.lock() {
            Ok(g) => g,
            Err(err) => err.into_inner(),
        };
        if guard.is_none() {
            match Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ) {
                Ok(conn) => {
                    *guard = Some(conn);
                }
                Err(err) => {
                    // Read-only DB-open failure means EVERY downstream
                    // read returns the empty placeholder, so the sidebar
                    // renders empty with no signal. Log so an operator
                    // can see the schema/permission/locked-DB cause
                    // without attaching a debugger.
                    eprintln!(
                        "[kilo-store] with_db: failed to open {path}: {err}",
                        path = path.display(),
                    );
                    return None;
                }
            }
        }
        let conn = guard.as_ref().expect("reader connection just initialized");
        Some(f(conn))
    }

    /// Audit Fix 9: cached writer connection. Previously, every write
    /// opened a fresh `rusqlite::Connection` and re-stamped pragmas — at
    /// hundreds of writes per turn the open/close churn was a measurable
    /// fraction of the wall-clock time. We now lazily open one writer and
    /// reuse it under a `Mutex` for the lifetime of the `Store`.
    fn with_write<T>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut guard = self.writer.lock().unwrap_or_else(|err| err.into_inner());
        if guard.is_none() {
            let path = self.paths.data.join("kilo.db");
            if let Some(dir) = path.parent() {
                fs::create_dir_all(dir)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
            }
            let mut conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            conn.execute_batch(
                "pragma journal_mode = WAL; pragma busy_timeout = 5000; pragma foreign_keys = ON;",
            )?;
            // First-run schema bootstrap. `init_schema` uses
            // `create table if not exists` so it is safe to run on every
            // open. The migration runner then walks `PRAGMA user_version`
            // forward to the binary's `LATEST_SCHEMA_VERSION` so old
            // databases upgrade in place. Together these two steps
            // satisfy **Storage and process self-healing invariants** 1
            // and 2 — bootstrap on missing/partial state, version-walk on
            // stale state.
            init_schema(&conn)?;
            migrations::run(&mut conn)?;
            *guard = Some(conn);
        }
        let conn = guard.as_mut().expect("writer connection initialized above");
        f(conn)
    }
}

fn init_schema(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "create table if not exists project (
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
    )
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Paths {
    fn new() -> Self {
        let home = env::var("KILO_TEST_HOME")
            .ok()
            .or_else(|| env::var("HOME").ok())
            .or_else(|| env::var("USERPROFILE").ok())
            .map(|value| PathBuf::from(value.trim()))
            .unwrap_or_else(|| PathBuf::from("."));

        Self {
            data: base("XDG_DATA_HOME", &home, Kind::Data).join("kilo"),
            config: base("XDG_CONFIG_HOME", &home, Kind::Config).join("kilo"),
            state: base("XDG_STATE_HOME", &home, Kind::State).join("kilo"),
            home,
        }
    }
}

enum Kind {
    Data,
    Config,
    State,
}

fn base(key: &str, home: &PathBuf, kind: Kind) -> PathBuf {
    if let Ok(value) = env::var(key) {
        return PathBuf::from(value.trim());
    }

    if cfg!(target_os = "windows") {
        return match kind {
            Kind::Data => appdata("LOCALAPPDATA", home, ["AppData", "Local"]),
            Kind::Config => appdata("APPDATA", home, ["AppData", "Roaming"]),
            Kind::State => appdata("LOCALAPPDATA", home, ["AppData", "Local"]),
        };
    }

    match kind {
        Kind::Data => home.join(".local").join("share"),
        Kind::Config => home.join(".config"),
        Kind::State => home.join(".local").join("state"),
    }
}

fn appdata<const N: usize>(key: &str, home: &PathBuf, parts: [&str; N]) -> PathBuf {
    if let Ok(value) = env::var(key) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    parts
        .iter()
        .fold(home.clone(), |path, part| path.join(part))
}

/// File priority for `GET /global/config` and `PATCH /global/config`.
///
/// Mirrors Bun's `globalConfigFile()` in `packages/opencode/src/config/config.ts:345`:
/// pick the *first* existing file in this order. Bun's full list also
/// includes `.jsonc` variants — those require a JSONC parser and are
/// deferred until M11. JSON-only here.
///
/// `kilo.json` is the highest priority so a user-authored Kilo-named file
/// wins over a leftover `opencode.json` from upstream. If none exist, the
/// first entry (`kilo.json`) is the fresh-install default.
fn config_priority() -> &'static [&'static str] {
    &["kilo.json", "opencode.json", "config.json"]
}

fn read_config(dir: &PathBuf) -> Config {
    for name in config_priority() {
        let path = dir.join(name);
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(JsonValue::Object(next)) = serde_json::from_str::<JsonValue>(&text) else {
            continue;
        };
        return Config {
            data: next.into_iter().collect(),
        };
    }

    Config {
        data: BTreeMap::new(),
    }
}

fn read_auths(dir: &PathBuf) -> BTreeMap<String, JsonValue> {
    let path = dir.join("auth.json");
    read_auth_file(&path)
        .or_else(|| read_auth_file(&auth_backup_path(&path)))
        .unwrap_or_default()
}

fn write_auths(dir: &PathBuf, data: &BTreeMap<String, JsonValue>) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = dir.join("auth.json");
    let body = serde_json::to_string_pretty(data)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    write_replace(&path, body.as_bytes())
}

fn read_auth_file(path: &Path) -> Option<BTreeMap<String, JsonValue>> {
    let text = fs::read_to_string(path).ok()?;
    let JsonValue::Object(data) = serde_json::from_str::<JsonValue>(&text).ok()? else {
        return None;
    };
    Some(data.into_iter().collect())
}

fn auth_backup_path(path: &Path) -> PathBuf {
    path.with_extension("json.bak")
}

fn auth_temp_path(path: &Path) -> PathBuf {
    let seq = AUTH_WRITES.fetch_add(1, atomic::Ordering::Relaxed);
    path.with_extension(format!("json.tmp.{}.{}", std::process::id(), seq))
}

/// Write auth data through a temp file and keep the previous valid file as
/// `auth.json.bak`. On Windows `std::fs::rename` cannot replace an existing
/// file, so the backup gives `read_auths` a recovery point if the process dies
/// between removing the old file and installing the new one.
fn write_replace(path: &Path, body: &[u8]) -> std::io::Result<()> {
    let temp = auth_temp_path(path);
    let bak = auth_backup_path(path);
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(body)?;
        file.sync_all()?;
    }
    if path.exists() {
        let _ = fs::copy(path, &bak);
        fs::remove_file(path)?;
    }
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = fs::remove_file(&temp);
            if !path.exists() && bak.exists() {
                let _ = fs::copy(&bak, path);
            }
            Err(err)
        }
    }
}

fn read_mcp_auths(dir: &PathBuf) -> BTreeMap<String, JsonValue> {
    let path = dir.join("mcp-auth.json");
    let Ok(text) = fs::read_to_string(&path) else {
        return BTreeMap::new();
    };
    let Ok(JsonValue::Object(data)) = serde_json::from_str::<JsonValue>(&text) else {
        return BTreeMap::new();
    };
    data.into_iter().collect()
}

fn read_permission_rules(dir: &PathBuf) -> Vec<JsonValue> {
    let path = dir.join("permissions.json");
    let Ok(text) = fs::read_to_string(&path) else {
        return Vec::new();
    };
    let Ok(JsonValue::Array(data)) = serde_json::from_str::<JsonValue>(&text) else {
        return Vec::new();
    };
    data
}

fn write_permission_rules(dir: &PathBuf, rules: &[JsonValue]) -> std::io::Result<()> {
    let path = dir.join("permissions.json");
    let body = serde_json::to_string_pretty(rules)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, body)
}

fn write_mcp_auths(dir: &PathBuf, data: &BTreeMap<String, JsonValue>) -> std::io::Result<()> {
    let path = dir.join("mcp-auth.json");
    let body = serde_json::to_string_pretty(data)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, body)
}

const PROJECT_COLUMNS: &str = "id, worktree, vcs, name, time_created, time_updated, \
    time_initialized, sandboxes, icon_url, icon_url_override, icon_color, commands";

/// Log-and-discard a rusqlite error from a read-path helper. Reads return
/// `Option`/`Vec` because callers use the value to render UI; on failure
/// (most often: schema mismatch when Bun adds a column we don't know
/// about yet, or DB locked by a concurrent writer) we want the operator
/// to *see* the cause instead of staring at an empty sidebar.
fn log_db_err(scope: &str, err: rusqlite::Error) {
    eprintln!("[kilo-store] {scope}: {err}");
}

fn read_project_by_worktree(db: &Connection, worktree: &str) -> Option<Project> {
    // Bun's `project` table — see `packages/opencode/src/project/project.sql.ts`.
    // The vcs column is a literal "git" or NULL, not an object.
    let sql = format!("select {PROJECT_COLUMNS} from project where worktree = ?1 limit 1");
    let mut stmt = match db.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_project_by_worktree.prepare", err);
            return None;
        }
    };
    let mut rows = match stmt.query_map([worktree], project_row) {
        Ok(rows) => rows,
        Err(err) => {
            log_db_err("read_project_by_worktree.query_map", err);
            return None;
        }
    };
    rows.next().and_then(Result::ok)
}

fn read_project_by_id(db: &Connection, id: &str) -> Option<Project> {
    let sql = format!("select {PROJECT_COLUMNS} from project where id = ?1 limit 1");
    let mut stmt = match db.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_project_by_id.prepare", err);
            return None;
        }
    };
    let mut rows = match stmt.query_map([id], project_row) {
        Ok(rows) => rows,
        Err(err) => {
            log_db_err("read_project_by_id.query_map", err);
            return None;
        }
    };
    rows.next().and_then(Result::ok)
}

fn project_row(row: &Row<'_>) -> rusqlite::Result<Project> {
    let sandboxes_json: Option<String> = row.get(7)?;
    let sandboxes: Vec<String> = sandboxes_json
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default();
    let icon_url: Option<String> = row.get(8)?;
    let icon_override: Option<String> = row.get(9)?;
    let icon_color: Option<String> = row.get(10)?;
    let icon = (icon_url.is_some() || icon_override.is_some() || icon_color.is_some()).then(|| {
        ProjectIcon {
            url: icon_url,
            override_url: icon_override,
            color: icon_color,
        }
    });
    // Bun stores commands as a JSON object (e.g. `{ "start": "..." }`). NULL
    // and empty objects both map to None on the wire so the SDK omits the field.
    let commands_json: Option<String> = row.get(11)?;
    let commands = commands_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<JsonValue>(raw).ok())
        .and_then(|value| match value {
            JsonValue::Object(map) if !map.is_empty() => Some(JsonValue::Object(map)),
            _ => None,
        })
        .map(|value| ProjectCommands {
            start: value
                .get("start")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
        });
    Ok(Project {
        id: row.get(0)?,
        worktree: row.get(1)?,
        vcs: row.get(2)?,
        name: row.get(3)?,
        icon,
        commands,
        time: Time {
            created: row.get(4)?,
            updated: row.get(5)?,
            initialized: row.get(6)?,
        },
        sandboxes,
    })
}

fn ensure_project(db: &Connection, worktree: &str, time: i64) -> rusqlite::Result<Project> {
    if let Some(project) = read_project_by_worktree(db, worktree) {
        return Ok(project);
    }
    if let Some(project) = read_project_by_id(db, "global") {
        return Ok(project);
    }

    db.execute(
        "insert into project (id, worktree, time_created, time_updated, sandboxes) \
         values ('global', ?1, ?2, ?3, '[]')",
        params![worktree, time, time],
    )?;
    Ok(Project {
        id: "global".to_string(),
        worktree: worktree.to_string(),
        vcs: None,
        name: None,
        icon: None,
        commands: None,
        time: Time {
            created: time,
            updated: time,
            initialized: None,
        },
        sandboxes: vec![],
    })
}

fn ensure_table(db: &Connection, name: &str) -> rusqlite::Result<()> {
    let count: i64 = db.query_row(
        "select count(*) from sqlite_master where type = 'table' and name = ?1",
        [name],
        |row| row.get(0),
    )?;
    if count > 0 {
        return Ok(());
    }

    Err(rusqlite::Error::InvalidParameterName(format!(
        "missing table {name}"
    )))
}

fn read_sessions(db: &Connection, query: &SessionQuery) -> Vec<Session> {
    let limit = query.limit.unwrap_or(100).min(500);
    let mut sql = String::from(
        "select id, project_id, workspace_id, parent_id, slug, directory, title, version, share_url, \
         summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, \
         time_created, time_updated, time_compacting, time_archived from session where 1 = 1",
    );
    let mut args = Vec::new();

    if let Some(dir) = query.directory.as_deref() {
        sql.push_str(" and directory = ?");
        args.push(SqlValue::Text(dir.to_string()));
    }
    if query.roots {
        sql.push_str(" and parent_id is null");
    }
    if let Some(start) = query.start {
        sql.push_str(" and time_updated >= ?");
        args.push(SqlValue::Integer(start));
    }
    if let Some(search) = query.search.as_deref() {
        sql.push_str(" and title like ?");
        args.push(SqlValue::Text(format!("%{search}%")));
    }
    sql.push_str(" order by time_updated desc limit ?");
    args.push(SqlValue::Integer(limit as i64));

    let mut stmt = match db.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_sessions.prepare", err);
            return vec![];
        }
    };

    let rows = match stmt.query_map(params_from_iter(args.iter()), session) {
        Ok(rows) => rows,
        Err(err) => {
            log_db_err("read_sessions.query_map", err);
            return vec![];
        }
    };

    rows.filter_map(Result::ok).collect()
}

fn count_sessions(db: &Connection, directory: Option<&str>) -> usize {
    let (sql, args): (&str, Vec<SqlValue>) = match directory {
        Some(dir) => (
            "select count(*) from session where directory = ?1",
            vec![SqlValue::Text(dir.to_string())],
        ),
        None => ("select count(*) from session", vec![]),
    };
    let mut stmt = match db.prepare(sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("count_sessions.prepare", err);
            return 0;
        }
    };
    let count: i64 = match stmt.query_row(params_from_iter(args.iter()), |row| row.get(0)) {
        Ok(n) => n,
        Err(err) => {
            log_db_err("count_sessions.query_row", err);
            return 0;
        }
    };
    usize::try_from(count.max(0)).unwrap_or(0)
}

fn read_session(db: &Connection, id: &str) -> Option<Session> {
    let mut stmt = match db.prepare(
        "select id, project_id, workspace_id, parent_id, slug, directory, title, version, share_url, \
         summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, \
         time_created, time_updated, time_compacting, time_archived from session where id = ?1",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_session.prepare", err);
            return None;
        }
    };

    match stmt.query_row([id], session) {
        Ok(value) => Some(value),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(err) => {
            log_db_err("read_session.query_row", err);
            None
        }
    }
}

fn read_children(db: &Connection, id: &str, worktree: &str) -> Option<Vec<Session>> {
    let project = read_project_by_worktree(db, worktree).map(|project| project.id);
    let mut sql = String::from(
        "select id, project_id, workspace_id, parent_id, slug, directory, title, version, share_url, \
         summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, \
         time_created, time_updated, time_compacting, time_archived from session where parent_id = ?1",
    );
    if project.is_some() {
        sql.push_str(" and project_id = ?2");
    }
    sql.push_str(" order by time_updated desc");
    let mut stmt = match db.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_children.prepare", err);
            return None;
        }
    };
    let rows = if let Some(project) = project {
        stmt.query_map(params![id, project], session)
    } else {
        stmt.query_map(params![id], session)
    };
    let rows = match rows {
        Ok(rows) => rows,
        Err(err) => {
            log_db_err("read_children.query_map", err);
            return None;
        }
    };
    Some(rows.filter_map(Result::ok).collect())
}

fn read_messages(
    db: &Connection,
    id: &str,
    limit: Option<usize>,
    before: Option<&MessageCursor>,
) -> Option<MessagePage> {
    read_session(db, id)?;
    let limit = limit.unwrap_or(0).min(500);
    let mut sql = String::from(
        "select id, session_id, time_created, time_updated, data from message where session_id = ?1",
    );
    if before.is_some() && limit > 0 {
        sql.push_str(" and (time_created < ?2 or (time_created = ?2 and id < ?3))");
    }
    sql.push_str(" order by time_created desc, id desc");
    if limit > 0 {
        sql.push_str(" limit ");
        sql.push_str(&(limit + 1).to_string());
    }

    let mut stmt = match db.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_messages.prepare", err);
            return None;
        }
    };
    let rows: Vec<MessageRow> = if let Some(cursor) = before.filter(|_| limit > 0) {
        match stmt.query_map((id, cursor.time, &cursor.id), |row| message(db, row)) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(err) => {
                log_db_err("read_messages.query_map.before", err);
                return None;
            }
        }
    } else {
        match stmt.query_map([id], |row| message(db, row)) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(err) => {
                log_db_err("read_messages.query_map.head", err);
                return None;
            }
        }
    };

    let mut rows = rows;
    let more = limit > 0 && rows.len() > limit;
    if more {
        rows.truncate(limit);
    }
    let cursor = if more {
        rows.last().map(|row| row.cursor.clone())
    } else {
        None
    };
    let mut items: Vec<Message> = rows.into_iter().map(|row| row.message).collect();
    items.reverse();
    Some(MessagePage {
        items,
        cursor,
        more,
    })
}

fn read_message(db: &Connection, id: &str, mid: &str) -> Option<Message> {
    let mut stmt = match db.prepare(
        "select id, session_id, time_created, time_updated, data from message where session_id = ?1 and id = ?2",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_message.prepare", err);
            return None;
        }
    };
    match stmt.query_row(params![id, mid], |row| message(db, row)) {
        Ok(row) => Some(row.message),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(err) => {
            log_db_err("read_message.query_row", err);
            None
        }
    }
}

fn read_part(db: &Connection, id: &str, mid: &str, pid: &str) -> Option<JsonValue> {
    let mut stmt = match db.prepare(
        "select id, message_id, session_id, data from part where session_id = ?1 and message_id = ?2 and id = ?3",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("read_part.prepare", err);
            return None;
        }
    };
    match stmt.query_row(params![id, mid, pid], |row| part(row)) {
        Ok(part) => Some(part),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(err) => {
            log_db_err("read_part.query_row", err);
            None
        }
    }
}

fn session(row: &Row<'_>) -> rusqlite::Result<Session> {
    let summary = summary(
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get::<_, Option<String>>(12)?,
    );
    let share = row
        .get::<_, Option<String>>(8)?
        .map(|url| json!({ "url": url }));

    Ok(Session {
        id: row.get(0)?,
        project_id: row.get(1)?,
        workspace_id: row.get(2)?,
        parent_id: row.get(3)?,
        slug: row.get(4)?,
        directory: row.get(5)?,
        title: row.get(6)?,
        version: row.get(7)?,
        summary,
        share,
        revert: json_opt(row.get(13)?),
        permission: json_opt(row.get(14)?),
        time: SessionTime {
            created: row.get(15)?,
            updated: row.get(16)?,
            compacting: row.get(17)?,
            archived: row.get(18)?,
        },
    })
}

fn insert_session(db: &Connection, session: &Session) -> rusqlite::Result<()> {
    db.execute(
        "insert into session (id, project_id, workspace_id, parent_id, slug, directory, title, version, \
         share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, \
         permission, time_created, time_updated, time_compacting, time_archived) \
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, null, null, null, null, null, null, ?9, ?10, ?11, null, null)",
        params![
            &session.id,
            &session.project_id,
            &session.workspace_id,
            &session.parent_id,
            &session.slug,
            &session.directory,
            &session.title,
            &session.version,
            json_text(&session.permission),
            session.time.created,
            session.time.updated,
        ],
    )?;
    Ok(())
}

fn message(db: &Connection, row: &Row<'_>) -> rusqlite::Result<MessageRow> {
    let id: String = row.get(0)?;
    let sid: String = row.get(1)?;
    let time: i64 = row.get(2)?;
    let data: String = row.get(4)?;
    let mut info = object(data);
    info.insert("id".to_string(), JsonValue::String(id.clone()));
    info.insert("sessionID".to_string(), JsonValue::String(sid.clone()));

    Ok(MessageRow {
        message: Message {
            info: JsonValue::Object(info),
            parts: parts(db, &id),
        },
        cursor: MessageCursor { id, time },
    })
}

fn parts(db: &Connection, id: &str) -> Vec<JsonValue> {
    // Insertion-order semantics: sort by `time_created` (the row's first
    // insertion timestamp; upserts keep it stable) so consumers see parts
    // in the order they were appended by the agent loop. `id` is the
    // tiebreaker when multiple parts land in the same millisecond, which
    // happens during fast streaming bursts.
    let mut stmt = match db.prepare(
        "select id, message_id, session_id, data from part where message_id = ?1 order by time_created, id",
    ) {
        Ok(stmt) => stmt,
        Err(err) => {
            log_db_err("parts.prepare", err);
            return vec![];
        }
    };
    let rows = match stmt.query_map([id], part) {
        Ok(rows) => rows,
        Err(err) => {
            log_db_err("parts.query_map", err);
            return vec![];
        }
    };

    rows.filter_map(Result::ok).collect()
}

fn part(row: &Row<'_>) -> rusqlite::Result<JsonValue> {
    let id: String = row.get(0)?;
    let mid: String = row.get(1)?;
    let sid: String = row.get(2)?;
    let data: String = row.get(3)?;
    let mut part = object(data);
    part.insert("id".to_string(), JsonValue::String(id));
    part.insert("messageID".to_string(), JsonValue::String(mid));
    part.insert("sessionID".to_string(), JsonValue::String(sid));
    Ok(JsonValue::Object(part))
}

fn session_tool_diffs(db: &Connection, id: &str) -> Vec<JsonValue> {
    let Some(page) = read_messages(db, id, None, None) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for msg in page.items {
        for part in msg.parts {
            if part.get("type").and_then(JsonValue::as_str) != Some("tool") {
                continue;
            }
            if part.pointer("/state/status").and_then(JsonValue::as_str) != Some("completed") {
                continue;
            }
            if let Some(item) = tool_diff(&part) {
                out.push(item);
            }
        }
    }
    out
}

fn tool_diff(part: &JsonValue) -> Option<JsonValue> {
    let tool = part.get("tool").and_then(JsonValue::as_str)?;
    let meta = part.pointer("/state/metadata")?;
    if tool == "apply_patch" {
        let files = meta.get("files")?.as_array()?;
        return Some(json!({
            "tool": tool,
            "callID": part.get("callID").cloned().unwrap_or(JsonValue::Null),
            "files": files,
            "patch": meta.get("diff").cloned().unwrap_or_else(|| JsonValue::String(String::new())),
        }));
    }

    let patch = meta.get("diff").or_else(|| meta.get("filediff"))?.clone();
    let file = meta
        .get("file")
        .or_else(|| meta.get("filepath"))
        .or_else(|| meta.get("path"))
        .cloned()
        .or_else(|| part.pointer("/state/input/filePath").cloned())
        .unwrap_or_else(|| JsonValue::String(tool.to_string()));
    Some(json!({
        "file": file,
        "patch": patch,
        "tool": tool,
        "callID": part.get("callID").cloned().unwrap_or(JsonValue::Null),
        "additions": meta.get("additions").cloned().unwrap_or_else(|| JsonValue::Number(0.into())),
        "deletions": meta.get("deletions").cloned().unwrap_or_else(|| JsonValue::Number(0.into())),
    }))
}

fn info_with_session(info: JsonValue, id: &str, time: i64) -> rusqlite::Result<JsonValue> {
    let mut info = ensure_object(info)?;
    info["sessionID"] = JsonValue::String(id.to_string());
    if info.get("time").is_none() {
        info["time"] = json!({ "created": time, "updated": time });
    }
    Ok(info)
}

fn write_part(
    db: &Connection,
    sid: &str,
    mid: &str,
    part: JsonValue,
    time: i64,
) -> rusqlite::Result<JsonValue> {
    let mut part = ensure_object(part)?;
    let pid = id_field(&part, "id").unwrap_or_else(|| part_id(time));
    part["id"] = JsonValue::String(pid.clone());
    part["sessionID"] = JsonValue::String(sid.to_string());
    part["messageID"] = JsonValue::String(mid.to_string());
    let data = data_without(part.clone(), ["id", "sessionID", "messageID"]);
    db.execute(
        "insert into part (id, message_id, session_id, time_created, time_updated, data) values (?1, ?2, ?3, ?4, ?5, ?6) \
         on conflict(id) do update set message_id = excluded.message_id, session_id = excluded.session_id, time_updated = excluded.time_updated, data = excluded.data",
        params![&pid, mid, sid, time, time, json_value(&data)?],
    )?;
    Ok(part)
}

fn write_event(
    db: &Connection,
    agg: &str,
    kind: &str,
    data: JsonValue,
) -> rusqlite::Result<StoredEvent> {
    if !has_table(db, "event_sequence")? || !has_table(db, "event")? {
        return Ok(StoredEvent {
            id: event_id(now_millis()),
            seq: -1,
            aggregate_id: agg.to_string(),
            data,
            event_type: kind.to_string(),
        });
    }

    let latest = db
        .query_row(
            "select seq from event_sequence where aggregate_id = ?1",
            [agg],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map_or(0, |seq| seq + 1);
    let id = event_id(now_millis());
    db.execute(
        "insert into event_sequence (aggregate_id, seq) values (?1, ?2) \
         on conflict(aggregate_id) do update set seq = excluded.seq",
        params![agg, latest],
    )?;
    db.execute(
        "insert into event (id, aggregate_id, seq, type, data) values (?1, ?2, ?3, ?4, ?5)",
        params![&id, agg, latest, kind, json_value(&data)?],
    )?;

    Ok(StoredEvent {
        id,
        seq: latest,
        aggregate_id: agg.to_string(),
        data,
        event_type: kind.to_string(),
    })
}

fn has_table(db: &Connection, name: &str) -> rusqlite::Result<bool> {
    let count: i64 = db.query_row(
        "select count(*) from sqlite_master where type = 'table' and name = ?1",
        [name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn ensure_object(value: JsonValue) -> rusqlite::Result<JsonValue> {
    if value.is_object() {
        return Ok(value);
    }

    Err(rusqlite::Error::InvalidParameterName(
        "expected json object".to_string(),
    ))
}

fn ensure_map(value: JsonValue) -> rusqlite::Result<Map<String, JsonValue>> {
    let JsonValue::Object(map) = value else {
        return Err(rusqlite::Error::InvalidParameterName(
            "expected json object".to_string(),
        ));
    };
    Ok(map)
}

fn required_str(data: &Map<String, JsonValue>, key: &str) -> rusqlite::Result<String> {
    data.get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| rusqlite::Error::InvalidParameterName(format!("missing {key}")))
}

fn opt_str(data: &Map<String, JsonValue>, key: &str) -> Option<String> {
    data.get(key)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn required_i64(data: &Map<String, JsonValue>, key: &str) -> rusqlite::Result<i64> {
    data.get(key)
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| rusqlite::Error::InvalidParameterName(format!("missing {key}")))
}

fn id_field(value: &JsonValue, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn data_without<const N: usize>(value: JsonValue, keys: [&str; N]) -> JsonValue {
    let JsonValue::Object(mut map) = value else {
        return JsonValue::Object(Map::new());
    };
    for key in keys {
        map.remove(key);
    }
    JsonValue::Object(map)
}

fn json_value(value: &JsonValue) -> rusqlite::Result<String> {
    serde_json::to_string(value)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

fn summary(
    additions: Option<i64>,
    deletions: Option<i64>,
    files: Option<i64>,
    diffs: Option<String>,
) -> Option<JsonValue> {
    if additions.is_none() && deletions.is_none() && files.is_none() {
        return None;
    }

    let mut data = json!({
        "additions": additions.unwrap_or(0),
        "deletions": deletions.unwrap_or(0),
        "files": files.unwrap_or(0),
    });
    if let Some(value) = json_opt(diffs) {
        data["diffs"] = value;
    }
    Some(data)
}

struct SummaryFields {
    additions: Option<i64>,
    deletions: Option<i64>,
    files: Option<i64>,
    diffs: Option<String>,
}

fn summary_fields(value: Option<&JsonValue>) -> SummaryFields {
    SummaryFields {
        additions: value
            .and_then(|value| value.get("additions"))
            .and_then(JsonValue::as_i64),
        deletions: value
            .and_then(|value| value.get("deletions"))
            .and_then(JsonValue::as_i64),
        files: value
            .and_then(|value| value.get("files"))
            .and_then(JsonValue::as_i64),
        diffs: value
            .and_then(|value| value.get("diffs"))
            .and_then(|value| serde_json::to_string(value).ok()),
    }
}

fn json_opt(text: Option<String>) -> Option<JsonValue> {
    text.and_then(|value| serde_json::from_str(&value).ok())
}

fn archived_time(time: Option<&JsonValue>) -> Option<i64> {
    time.and_then(|value| value.get("archived"))
        .and_then(JsonValue::as_i64)
}

fn object(text: String) -> Map<String, JsonValue> {
    match serde_json::from_str::<JsonValue>(&text).unwrap_or(JsonValue::Object(Map::new())) {
        JsonValue::Object(map) => map,
        _ => Map::new(),
    }
}

fn now_millis() -> i64 {
    let raw = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|time| time.as_millis() as i64)
        .unwrap_or_default();
    // Audit Fix 9 follow-up: ensure the wall-clock millis we hand to the
    // DB strictly increase across calls in the same process. Pre-Fix-9 the
    // open/close churn around each `with_write` gave us natural ms
    // separation; with the cached writer multiple consecutive writes can
    // land in the same ms, which breaks `order by time_created desc, id
    // desc` for callers that rely on insertion order (the message list
    // pages are one). Bumping to `last+1` whenever the clock hasn't moved
    // costs nothing in production but keeps insertion order stable.
    static LAST_MS: atomic::AtomicI64 = atomic::AtomicI64::new(0);
    loop {
        let prev = LAST_MS.load(atomic::Ordering::SeqCst);
        let next = if raw > prev { raw } else { prev + 1 };
        match LAST_MS.compare_exchange(
            prev,
            next,
            atomic::Ordering::SeqCst,
            atomic::Ordering::SeqCst,
        ) {
            Ok(_) => return next,
            Err(_) => continue,
        }
    }
}

fn default_title(time: i64) -> String {
    let date = DateTime::<Utc>::from_timestamp_millis(time).unwrap_or_else(Utc::now);
    format!(
        "New session - {}",
        date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    )
}

fn fork_title(title: &str) -> String {
    let Some((base, rest)) = title.rsplit_once(" (fork #") else {
        return format!("{title} (fork #1)");
    };
    let Some(num) = rest.strip_suffix(')') else {
        return format!("{title} (fork #1)");
    };
    let Ok(num) = num.parse::<usize>() else {
        return format!("{title} (fork #1)");
    };
    format!("{base} (fork #{})", num + 1)
}

/// Base62 alphabet matching Bun's `Identifier.randomBase62` in
/// [`packages/shared/src/util/identifier.ts`](../../../shared/src/util/identifier.ts).
/// Order matters — index N must produce the same character Bun produces
/// for the same byte value, otherwise lexicographic ordering across
/// Rust- and Bun-created IDs in the same millisecond/counter slot drifts.
const BASE62: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// 14 random base62 chars, matching Bun's ID tail. Bun samples from
/// `crypto.randomBytes(14)` then `% 62` per byte; we mirror that exactly
/// so collision behavior and lexicographic distribution match.
fn random_tail() -> String {
    let mut bytes = [0u8; 14];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| BASE62[(*byte as usize) % 62] as char)
        .collect()
}

/// Build an ID with the Bun shape: `{prefix}_{12 hex of clock}{14 random base62}`.
/// `descending` flips the clock bits so newer IDs sort *earlier* than older
/// ones — used for sessions/messages/parts so the most recent is at the
/// top of `ORDER BY id`. Events use ascending order so replay reads in
/// chronological order. See [`packages/shared/src/util/identifier.ts:28-47`](../../../shared/src/util/identifier.ts#L28-L47).
fn make_id(prefix: &str, time: i64, descending: bool) -> String {
    let seq = IDS.fetch_add(1, atomic::Ordering::Relaxed) & 0xfff;
    let now = (time as u64).wrapping_mul(0x1000).wrapping_add(seq);
    let clock = if descending { !now } else { now };
    format!(
        "{prefix}_{clock:012x}{tail}",
        clock = clock & 0xffffffffffff,
        tail = random_tail(),
    )
}

fn session_id(time: i64) -> String {
    make_id("ses", time, true)
}

fn message_id(time: i64) -> String {
    make_id("msg", time, true)
}

fn part_id(time: i64) -> String {
    make_id("prt", time, true)
}

fn event_id(time: i64) -> String {
    make_id("evt", time, false)
}

fn slug(id: &str) -> String {
    id.to_string()
}

fn json_text(value: &Option<JsonValue>) -> Option<String> {
    value
        .as_ref()
        .and_then(|value| serde_json::to_string(value).ok())
}

fn non_null(value: JsonValue) -> Option<JsonValue> {
    (!value.is_null()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_deletes_session() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);

        let session = store
            .create_session(SessionCreateInput {
                title: Some("Write test".to_string()),
                permission: Some(json!({ "edit": "allow" })),
                workspace_id: Some("wrk_test".to_string()),
                ..Default::default()
            })
            .expect("create session");

        assert!(session.id.starts_with("ses_"));
        assert_eq!(session.project_id, "global");
        assert_eq!(session.title, "Write test");
        assert_eq!(session.permission, Some(json!({ "edit": "allow" })));
        assert_eq!(store.session(&session.id).unwrap().id, session.id);

        let deleted = store
            .delete_session(&session.id)
            .expect("delete session")
            .expect("pre-delete session");

        assert_eq!(deleted.id, session.id);
        assert!(store.session(&session.id).is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn provider_auth_roundtrips_auth_json_and_preserves_neighbors() {
        let root = unique_root();
        let store = store(&root);
        fs::create_dir_all(&store.paths.data).unwrap();
        fs::write(
            store.paths.data.join("auth.json"),
            serde_json::to_string(&json!({
                "other": { "type": "api", "key": "keep", "extra": true },
                "openai/": { "type": "api", "key": "old" }
            }))
            .unwrap(),
        )
        .unwrap();

        let auth = json!({
            "type": "oauth",
            "refresh": "refresh-token",
            "access": "access-token",
            "expires": 123,
            "accountId": "acct_1",
            "enterpriseUrl": "https://enterprise.test",
            "unknown": { "kept": true }
        });
        store
            .set_provider_auth("openai", auth.clone())
            .expect("set auth");
        let all = store.provider_auths();

        assert_eq!(all.get("openai"), Some(&auth));
        assert!(all.get("openai/").is_none());
        assert_eq!(all["other"]["extra"], true);
        assert_eq!(store.provider_auth("openai"), Some(auth));

        store.clear_provider_auth("openai").expect("clear auth");
        let all = store.provider_auths();
        assert!(all.get("openai").is_none());
        assert_eq!(all["other"]["key"], "keep");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn provider_auth_recovers_from_backup_when_auth_json_is_corrupt() {
        let root = unique_root();
        let store = store(&root);
        fs::create_dir_all(&store.paths.data).unwrap();
        let auth = json!({
            "openai": {
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "access-token",
                "expires": 123
            }
        });
        fs::write(store.paths.data.join("auth.json"), b"{\"openai\":").unwrap();
        fs::write(
            store.paths.data.join("auth.json.bak"),
            serde_json::to_string(&auth).unwrap(),
        )
        .unwrap();

        let all = store.provider_auths();
        assert_eq!(all["openai"]["refresh"], "refresh-token");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn provider_auth_writes_backup_before_replacing_auth_json() {
        let root = unique_root();
        let store = store(&root);
        fs::create_dir_all(&store.paths.data).unwrap();
        fs::write(
            store.paths.data.join("auth.json"),
            serde_json::to_string(&json!({
                "openai": {
                    "type": "oauth",
                    "refresh": "old-refresh",
                    "access": "old-access",
                    "expires": 1
                }
            }))
            .unwrap(),
        )
        .unwrap();

        store
            .set_provider_auth(
                "openai",
                json!({
                    "type": "oauth",
                    "refresh": "new-refresh",
                    "access": "new-access",
                    "expires": 123
                }),
            )
            .expect("set auth");
        let all = store.provider_auths();
        let bak = fs::read_to_string(store.paths.data.join("auth.json.bak")).unwrap();
        let prev: JsonValue = serde_json::from_str(&bak).unwrap();

        assert_eq!(all["openai"]["refresh"], "new-refresh");
        assert_eq!(prev["openai"]["refresh"], "old-refresh");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn create_session_initializes_schema_on_empty_db() {
        let root = unique_root();
        let store = store(&root);
        fs::create_dir_all(&store.paths.data).unwrap();
        Connection::open(store.paths.data.join("kilo.db")).unwrap();

        store
            .create_session(SessionCreateInput::default())
            .expect("create_session should bootstrap schema on an empty db file");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn updates_session_title_permission_and_archived_time() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let updated = store
            .update_session(
                &session.id,
                SessionUpdateInput {
                    title: Some("Archived work".to_string()),
                    permission: Some(json!({ "edit": "allow", "bash": "ask" })),
                    time: Some(json!({ "archived": 123 })),
                },
            )
            .expect("update session")
            .expect("updated session");

        assert_eq!(updated.title, "Archived work");
        assert_eq!(
            updated.permission,
            Some(json!({ "edit": "allow", "bash": "ask" }))
        );
        assert_eq!(updated.time.archived, Some(123));
        assert!(updated.time.updated >= session.time.updated);

        let db = Connection::open(store.paths.data.join("kilo.db")).unwrap();
        let row: (String, i64) = db
            .query_row(
                "select permission, time_archived from session where id = ?1",
                [&session.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<JsonValue>(&row.0).unwrap(),
            json!({ "edit": "allow", "bash": "ask" })
        );
        assert_eq!(row.1, 123);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn update_session_noop_returns_existing_session() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput {
                title: Some("Keep me".to_string()),
                permission: Some(json!({ "edit": "ask" })),
                ..Default::default()
            })
            .expect("create session");

        let updated = store
            .update_session(
                &session.id,
                SessionUpdateInput {
                    time: Some(JsonValue::Null),
                    ..Default::default()
                },
            )
            .expect("noop update")
            .expect("existing session");

        assert_eq!(updated.title, session.title);
        assert_eq!(updated.permission, session.permission);
        assert_eq!(updated.time.updated, session.time.updated);
        assert_eq!(updated.time.archived, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn appends_message_and_parts_without_metadata_in_data() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({
                        "id": "msg_test",
                        "sessionID": "wrong",
                        "role": "user",
                        "time": { "created": 1, "updated": 2 }
                    }),
                    parts: vec![json!({
                        "id": "prt_test",
                        "sessionID": "wrong",
                        "messageID": "wrong",
                        "type": "text",
                        "text": "hello"
                    })],
                },
            )
            .expect("append message");

        assert_eq!(out.info["id"], "msg_test");
        assert_eq!(out.info["sessionID"], session.id);
        assert_eq!(out.parts[0]["id"], "prt_test");
        assert_eq!(out.parts[0]["messageID"], "msg_test");
        assert_eq!(out.parts[0]["sessionID"], session.id);

        let db = Connection::open(store.paths.data.join("kilo.db")).unwrap();
        let data: String = db
            .query_row(
                "select data from message where id = 'msg_test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let data: JsonValue = serde_json::from_str(&data).unwrap();
        assert!(data.get("id").is_none());
        assert!(data.get("sessionID").is_none());
        assert_eq!(data["role"], "user");

        let data: String = db
            .query_row("select data from part where id = 'prt_test'", [], |row| {
                row.get(0)
            })
            .unwrap();
        let data: JsonValue = serde_json::from_str(&data).unwrap();
        assert!(data.get("id").is_none());
        assert!(data.get("sessionID").is_none());
        assert!(data.get("messageID").is_none());
        assert_eq!(data["text"], "hello");

        let fresh = store.session(&session.id).unwrap();
        assert!(fresh.time.updated >= session.time.updated);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_allocates_missing_message_and_part_ids() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "role": "assistant" }),
                    parts: vec![json!({ "type": "step-start" })],
                },
            )
            .expect("append message");

        assert!(out.info["id"].as_str().unwrap().starts_with("msg_"));
        assert_eq!(out.info["sessionID"], session.id);
        assert!(out.info["time"].is_object());
        assert!(out.parts[0]["id"].as_str().unwrap().starts_with("prt_"));
        assert_eq!(out.parts[0]["messageID"], out.info["id"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_rejects_missing_session() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);

        let err = store
            .append_message(
                "ses_missing",
                MessageAppendInput {
                    info: json!({ "role": "user" }),
                    parts: vec![],
                },
            )
            .expect_err("missing session should fail");

        assert!(matches!(err, rusqlite::Error::QueryReturnedNoRows));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_persists_sync_events_in_order() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let record = store
            .append_message_record(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_sync", "role": "user" }),
                    parts: vec![
                        json!({ "id": "prt_a", "type": "text", "text": "a" }),
                        json!({ "id": "prt_b", "type": "text", "text": "b" }),
                    ],
                },
            )
            .expect("append message");

        assert_eq!(record.events[0].event_type, "message.updated.v1");
        assert_eq!(record.events[1].event_type, "message.part.updated.v1");
        assert_eq!(record.events[2].event_type, "message.part.updated.v1");
        assert_eq!(record.events[0].seq + 1, record.events[1].seq);
        assert_eq!(record.events[1].seq + 1, record.events[2].seq);
        assert_eq!(record.events[0].data["info"]["id"], "msg_sync");
        assert_eq!(record.events[1].data["part"]["id"], "prt_a");

        let db = Connection::open(store.paths.data.join("kilo.db")).unwrap();
        let seq: i64 = db
            .query_row(
                "select seq from event_sequence where aggregate_id = ?1",
                [&session.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(seq, 3);

        let rows = events(&db, &session.id);
        assert_eq!(rows[1].0, 1);
        assert_eq!(rows[1].1, "message.updated.v1");
        assert_eq!(rows[1].2["info"]["id"], "msg_sync");
        assert_eq!(rows[2].0, 2);
        assert_eq!(rows[2].1, "message.part.updated.v1");
        assert_eq!(rows[2].2["part"]["id"], "prt_a");
        assert_eq!(rows[3].0, 3);
        assert_eq!(rows[3].1, "message.part.updated.v1");
        assert_eq!(rows[3].2["part"]["id"], "prt_b");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn append_self_heals_missing_event_tables() {
        let root = unique_root();
        let store = store(&root);
        seed_without_events(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let record = store
            .append_message_record(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_no_events", "role": "user" }),
                    parts: vec![json!({ "id": "prt_no_events", "type": "text" })],
                },
            )
            .expect("append message");

        assert_eq!(record.events.len(), 2);
        assert!(
            record.events.iter().all(|e| e.seq >= 0),
            "init_schema should have created event tables; expected real seqs, got {:?}",
            record.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        );
        assert_eq!(record.result.info["id"], "msg_no_events");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn children_and_fork_clone_messages_before_cutoff() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput {
                title: Some("Base".to_string()),
                workspace_id: Some("wrk_1".to_string()),
                ..Default::default()
            })
            .expect("create session");
        store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_user", "role": "user" }),
                    parts: vec![json!({ "id": "prt_user", "type": "text", "text": "hello" })],
                },
            )
            .unwrap();
        store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_assistant", "role": "assistant", "parentID": "msg_user" }),
                    parts: vec![json!({ "id": "prt_assistant", "type": "text", "text": "hi" })],
                },
            )
            .unwrap();

        let record = store
            .fork_session_record(
                &session.id,
                SessionForkInput {
                    message_id: Some("msg_assistant".to_string()),
                },
            )
            .expect("fork session")
            .expect("forked session");

        assert_eq!(record.session.title, "Base (fork #1)");
        assert_eq!(record.session.parent_id, Some(session.id.clone()));
        assert_eq!(record.session.workspace_id, Some("wrk_1".to_string()));
        assert_eq!(record.events[0].event_type, "session.created.v1");
        assert_eq!(record.events[1].event_type, "message.updated.v1");
        assert_eq!(record.events[2].event_type, "message.part.updated.v1");
        let page = store.messages(&record.session.id, None, None).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].info["role"], "user");
        assert_ne!(page.items[0].info["id"], "msg_user");
        assert_eq!(page.items[0].parts[0]["text"], "hello");
        let children = store.children(&session.id).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, record.session.id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn message_part_mutations_persist_events() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_edit", "role": "user" }),
                    parts: vec![json!({ "id": "prt_edit", "type": "text", "text": "old" })],
                },
            )
            .unwrap();

        let updated = store
            .update_part_record(
                &session.id,
                "msg_edit",
                "prt_edit",
                json!({ "id": "prt_edit", "messageID": "msg_edit", "sessionID": session.id, "type": "text", "text": "new" }),
            )
            .expect("update part");
        assert_eq!(updated.part.unwrap()["text"], "new");
        assert_eq!(updated.events[0].event_type, "message.part.updated.v1");

        let removed = store
            .remove_part_record(&session.id, "msg_edit", "prt_edit")
            .expect("remove part");
        assert_eq!(removed.events[0].event_type, "message.part.removed.v1");
        let removed = store
            .remove_message_record(&session.id, "msg_edit")
            .expect("remove message");
        assert_eq!(removed.events[0].event_type, "message.removed.v1");
        assert!(store.message(&session.id, "msg_edit").is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn revert_share_and_diff_are_storage_backed() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let record = store
            .set_revert_record(
                &session.id,
                SessionRevertInput {
                    message_id: Some("msg_revert".to_string()),
                    summary: Some(json!({
                        "additions": 1,
                        "deletions": 2,
                        "files": 1,
                        "diffs": [{ "file": "a.txt", "additions": 1, "deletions": 2, "status": "modified" }]
                    })),
                    ..Default::default()
                },
            )
            .expect("set revert");
        let session = record.session.unwrap();
        assert_eq!(session.revert.unwrap()["messageID"], "msg_revert");
        assert_eq!(store.diff(&session.id).unwrap().len(), 1);
        assert_eq!(record.events[0].event_type, "session.updated.v1");

        let shared = store
            .set_share_record(&session.id, Some("https://share.test/s".to_string()))
            .expect("share")
            .session
            .unwrap();
        assert_eq!(shared.share.unwrap()["url"], "https://share.test/s");
        let unshared = store.clear_share_record(&session.id).expect("unshare");
        assert!(unshared.session.unwrap().share.is_none());
        let cleared = store.clear_revert_record(&session.id).expect("unrevert");
        assert!(cleared.session.unwrap().revert.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn diff_falls_back_to_rust_tool_metadata_without_checkpoint_summary() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_tool_diff", "role": "assistant" }),
                    parts: vec![json!({
                        "id": "prt_tool_diff",
                        "type": "tool",
                        "tool": "write",
                        "callID": "call_write_diff",
                        "state": {
                            "status": "completed",
                            "input": { "filePath": "note.txt", "content": "after\n" },
                            "output": "Wrote file successfully.",
                            "metadata": {
                                "file": "note.txt",
                                "diff": "--- note.txt\n+++ note.txt\n@@ before @@\n@@ after @@\n+after",
                                "additions": 1,
                                "deletions": 0
                            },
                            "title": "note.txt",
                            "time": { "start": 1, "end": 1 }
                        }
                    })],
                },
            )
            .unwrap();

        let diff = store.diff(&session.id).unwrap();

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0]["file"], "note.txt");
        assert_eq!(diff[0]["tool"], "write");
        assert_eq!(diff[0]["callID"], "call_write_diff");
        assert_eq!(diff[0]["additions"], 1);
        assert!(diff[0]["patch"].as_str().unwrap().contains("+after"));

        let _ = fs::remove_dir_all(root);
    }

    fn store(root: &std::path::Path) -> Store {
        Store {
            paths: Paths {
                home: root.join("home"),
                data: root.join("data").join("kilo"),
                config: root.join("config").join("kilo"),
                state: root.join("state").join("kilo"),
            },
            directory: root.join("repo").to_string_lossy().to_string(),
            worktree: root.join("repo").to_string_lossy().to_string(),
            writer: Arc::new(Mutex::new(None)),
            reader: Arc::new(Mutex::new(None)),
        }
    }

    fn events(db: &Connection, agg: &str) -> Vec<(i64, String, JsonValue)> {
        let mut stmt = db
            .prepare("select seq, type, data from event where aggregate_id = ?1 order by seq")
            .unwrap();
        stmt.query_map([agg], |row| {
            let data: String = row.get(2)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                serde_json::from_str(&data).unwrap(),
            ))
        })
        .unwrap()
        .filter_map(Result::ok)
        .collect()
    }

    fn seed(store: &Store) {
        fs::create_dir_all(&store.paths.data).unwrap();
        let db = Connection::open(store.paths.data.join("kilo.db")).unwrap();
        db.execute_batch(
            "create table project (
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
            create table session (
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
            create table message (
                id text primary key,
                session_id text not null references session(id) on delete cascade,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );
            create table part (
                id text primary key,
                message_id text not null references message(id) on delete cascade,
                session_id text not null,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );
            create table event_sequence (
                aggregate_id text not null primary key,
                seq integer not null
            );
            create table event (
                id text primary key,
                aggregate_id text not null references event_sequence(aggregate_id) on delete cascade,
                seq integer not null,
                type text not null,
                data text not null
            );",
        )
        .unwrap();
    }

    fn seed_without_events(store: &Store) {
        fs::create_dir_all(&store.paths.data).unwrap();
        let db = Connection::open(store.paths.data.join("kilo.db")).unwrap();
        db.execute_batch(
            "create table project (
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
            create table session (
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
            create table message (
                id text primary key,
                session_id text not null references session(id) on delete cascade,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );
            create table part (
                id text primary key,
                message_id text not null references message(id) on delete cascade,
                session_id text not null,
                time_created integer not null,
                time_updated integer not null,
                data text not null
            );",
        )
        .unwrap();
    }

    fn unique_root() -> PathBuf {
        let seq = IDS.fetch_add(1, atomic::Ordering::Relaxed);
        let name = format!("kilo-store-test-{}-{seq}", now_millis());
        env::temp_dir().join(name)
    }

    /// Audit Fix 9 smoke test: 100 sequential writes share a single
    /// cached connection. Pre-fix this opened/closed sqlite 100 times.
    #[test]
    fn cached_writer_handles_many_sequential_writes() {
        let root = unique_root();
        let store = store(&root);
        seed(&store);
        for i in 0..100 {
            let session = store
                .create_session(SessionCreateInput {
                    title: Some(format!("session {i}")),
                    ..Default::default()
                })
                .expect("create session");
            assert!(session.id.starts_with("ses_"));
        }
        let _ = fs::remove_dir_all(root);
    }

    /// **Storage and process self-healing invariants → 1**: a fresh
    /// install with no `kilo.db` on disk produces a working store on
    /// first chat. The pre-M6.1 bug shipped because schema bootstrap
    /// was gated on file existence; this test makes the gateless
    /// behavior load-bearing.
    #[test]
    fn fresh_install_creates_db_on_first_write() {
        let root = unique_root();
        let store = store(&root);
        let db_path = root.join("data").join("kilo").join("kilo.db");
        assert!(
            !db_path.exists(),
            "precondition: no kilo.db before first write"
        );

        // First write triggers schema bootstrap. No `seed()` call —
        // the production code path must self-heal.
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("first chat creates schema and persists session");
        assert!(session.id.starts_with("ses_"));
        assert!(db_path.exists(), "kilo.db is created after first write");

        // Reads succeed against the freshly created store.
        let listed = store.sessions(&SessionQuery::default());
        assert_eq!(listed.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    /// **Storage and process self-healing invariants → 1**: a half-init
    /// fixture (an empty `kilo.db` file plus orphaned `-wal` / `-shm`
    /// siblings, the on-disk shape a crashed init leaves behind)
    /// converges on first write rather than panicking.
    #[test]
    fn half_init_db_self_heals_on_first_write() {
        let root = unique_root();
        let data_dir = root.join("data").join("kilo");
        fs::create_dir_all(&data_dir).unwrap();
        // Create the on-disk artefacts of a crashed init: an empty
        // SQLite file and orphaned WAL/SHM siblings. SQLite tolerates
        // empty WAL/SHM and rebuilds them on next open.
        let db_path = data_dir.join("kilo.db");
        let wal_path = data_dir.join("kilo.db-wal");
        let shm_path = data_dir.join("kilo.db-shm");
        fs::write(&db_path, b"").unwrap();
        fs::write(&wal_path, b"").unwrap();
        fs::write(&shm_path, b"").unwrap();

        let store = store(&root);
        let session = store
            .create_session(SessionCreateInput::default())
            .expect("half-init store self-heals");
        assert!(session.id.starts_with("ses_"));
        // Schema is present and queryable.
        assert_eq!(
            store.sessions(&SessionQuery::default()).len(),
            1,
            "self-heal preserves the just-written session"
        );

        let _ = fs::remove_dir_all(root);
    }

    /// **Storage and process self-healing invariants → 4**: column-name
    /// parity between Rust's `init_schema` and Bun's persisted shape.
    /// Bun is the round-trip oracle until M14 step 7 removes it; if
    /// Rust ever reorders, renames, or omits a column the rollback
    /// window breaks silently. This freezes the column set so any
    /// schema change forces an explicit ack here AND in
    /// `migrations::MIGRATIONS`.
    #[test]
    fn schema_columns_match_bun_compat_set() {
        let root = unique_root();
        let store = store(&root);
        // First write triggers schema bootstrap.
        store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let conn = Connection::open(root.join("data").join("kilo").join("kilo.db")).unwrap();

        let assert_columns = |table: &str, expected: &[&str]| {
            let mut stmt = conn
                .prepare(&format!("pragma table_info({table})"))
                .unwrap();
            let actual: Vec<String> = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .filter_map(Result::ok)
                .collect();
            assert_eq!(
                actual,
                expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "{table} column drift — update CONTRACT.md and migrations.rs",
            );
        };

        assert_columns(
            "project",
            &[
                "id",
                "worktree",
                "vcs",
                "name",
                "icon_url",
                "icon_url_override",
                "icon_color",
                "time_created",
                "time_updated",
                "time_initialized",
                "sandboxes",
                "commands",
            ],
        );
        assert_columns(
            "session",
            &[
                "id",
                "project_id",
                "workspace_id",
                "parent_id",
                "slug",
                "directory",
                "title",
                "version",
                "share_url",
                "summary_additions",
                "summary_deletions",
                "summary_files",
                "summary_diffs",
                "revert",
                "permission",
                "time_created",
                "time_updated",
                "time_compacting",
                "time_archived",
            ],
        );
        assert_columns(
            "message",
            &["id", "session_id", "time_created", "time_updated", "data"],
        );
        assert_columns(
            "part",
            &[
                "id",
                "message_id",
                "session_id",
                "time_created",
                "time_updated",
                "data",
            ],
        );
        assert_columns("event_sequence", &["aggregate_id", "seq"]);
        assert_columns("event", &["id", "aggregate_id", "seq", "type", "data"]);

        let _ = fs::remove_dir_all(root);
    }
}
