use std::{path::PathBuf, sync::Arc};

use axum::{extract::State, Json};
use serde_json::Value;

use crate::{registry, AppState};

pub(crate) async fn skills(State(state): State<Arc<AppState>>) -> Json<Vec<Value>> {
    let paths = state.store.paths();
    Json(
        registry::skills(
            &PathBuf::from(paths.directory),
            &PathBuf::from(paths.config),
            &PathBuf::from(paths.home),
        )
        .iter()
        .map(registry::skill_json)
        .collect(),
    )
}

pub(crate) async fn commands(State(state): State<Arc<AppState>>) -> Json<Vec<Value>> {
    let paths = state.store.paths();
    Json(
        registry::commands(
            &PathBuf::from(paths.directory),
            &PathBuf::from(paths.config),
            &PathBuf::from(paths.home),
        )
        .iter()
        .map(registry::command_json)
        .collect(),
    )
}
