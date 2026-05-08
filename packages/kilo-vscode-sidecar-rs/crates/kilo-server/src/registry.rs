use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub(crate) struct SkillInfo {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) location: String,
    pub(crate) content: String,
}

#[derive(Clone, Debug)]
pub(crate) struct CommandInfo {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) agent: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) source: String,
    pub(crate) template: String,
    pub(crate) subtask: Option<bool>,
    pub(crate) hints: Vec<String>,
}

pub(crate) fn skills(root: &Path, config: &Path, home: &Path) -> Vec<SkillInfo> {
    let mut map = BTreeMap::new();
    for dir in dirs(root, config, home) {
        scan_skills(&dir, &mut map);
    }
    map.into_values().collect()
}

pub(crate) fn commands(root: &Path, config: &Path, home: &Path) -> Vec<CommandInfo> {
    let mut map = BTreeMap::new();
    for dir in dirs(root, config, home) {
        scan_commands(&dir, &mut map);
    }
    for skill in skills(root, config, home) {
        map.entry(skill.name.clone())
            .or_insert_with(|| CommandInfo {
                name: skill.name,
                description: Some(skill.description),
                agent: None,
                model: None,
                source: "skill".to_string(),
                template: skill.content,
                subtask: None,
                hints: Vec::new(),
            });
    }
    map.into_values().collect()
}

pub(crate) fn command(root: &Path, config: &Path, home: &Path, name: &str) -> Option<CommandInfo> {
    commands(root, config, home)
        .into_iter()
        .find(|cmd| cmd.name == name)
}

pub(crate) fn slash(text: &str) -> Option<(&str, &str)> {
    let text = text.trim();
    let rest = text.strip_prefix('/')?;
    if rest.is_empty() || rest.starts_with(char::is_whitespace) {
        return None;
    }
    let idx = rest.find(char::is_whitespace).unwrap_or(rest.len());
    Some((&rest[..idx], rest[idx..].trim()))
}

pub(crate) fn expand(template: &str, input: &str) -> String {
    let args = args(input);
    let mut last = 0usize;
    let bytes = template.as_bytes();
    let mut idx = 0usize;
    while idx + 1 < bytes.len() {
        if bytes[idx] == b'$' && bytes[idx + 1].is_ascii_digit() {
            let mut end = idx + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(num) = template[idx + 1..end].parse::<usize>() {
                last = last.max(num);
            }
            idx = end;
            continue;
        }
        idx += 1;
    }
    let mut out = String::new();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if idx + 1 < bytes.len() && bytes[idx] == b'$' && bytes[idx + 1].is_ascii_digit() {
            let mut end = idx + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let rep = template[idx + 1..end]
                .parse::<usize>()
                .ok()
                .and_then(|num| arg(&args, num, last))
                .unwrap_or_default();
            out.push_str(&rep);
            idx = end;
            continue;
        }
        out.push(bytes[idx] as char);
        idx += 1;
    }
    let used = template.contains("$ARGUMENTS");
    let out = out.replace("$ARGUMENTS", input);
    let out = if last == 0 && !used && !input.trim().is_empty() {
        format!("{out}\n\n{input}")
    } else {
        out
    };
    out.trim().to_string()
}

fn arg(args: &[String], num: usize, last: usize) -> Option<String> {
    let idx = num.checked_sub(1)?;
    if idx >= args.len() {
        return Some(String::new());
    }
    if num == last {
        return Some(args[idx..].join(" "));
    }
    Some(args[idx].clone())
}

fn args(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for ch in input.chars() {
        if quote == Some(ch) {
            quote = None;
            continue;
        }
        if quote.is_none() && (ch == '\'' || ch == '"') {
            quote = Some(ch);
            continue;
        }
        if quote.is_none() && ch.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        cur.push(ch);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

pub(crate) fn skills_prompt(list: &[SkillInfo]) -> Option<String> {
    Some(format!(
        "Skills provide specialized instructions and workflows for specific tasks.\n\
         Use the skill tool to load a skill when a task matches its description.\n{}",
        fmt_skills(list)
    ))
}

fn fmt_skills(list: &[SkillInfo]) -> String {
    if list.is_empty() {
        return "No skills are currently available.".to_string();
    }
    let mut out = vec!["<available_skills>".to_string()];
    for skill in list {
        out.push("  <skill>".to_string());
        out.push(format!("    <name>{}</name>", skill.name));
        out.push(format!(
            "    <description>{}</description>",
            skill.description
        ));
        out.push(format!("    <location>{}</location>", skill.location));
        out.push("  </skill>".to_string());
    }
    out.push("</available_skills>".to_string());
    out.join("\n")
}

pub(crate) fn skill_json(skill: &SkillInfo) -> Value {
    json!({ "name": skill.name, "description": skill.description, "location": skill.location, "content": skill.content })
}

pub(crate) fn command_json(command: &CommandInfo) -> Value {
    let mut value = json!({ "name": command.name, "source": command.source, "template": command.template, "hints": command.hints });
    if let Some(description) = &command.description {
        value["description"] = json!(description);
    }
    if let Some(agent) = &command.agent {
        value["agent"] = json!(agent);
    }
    if let Some(model) = &command.model {
        value["model"] = json!(model);
    }
    if let Some(subtask) = command.subtask {
        value["subtask"] = json!(subtask);
    }
    value
}

fn dirs(root: &Path, config: &Path, home: &Path) -> Vec<PathBuf> {
    let mut dirs = vec![config.to_path_buf()];
    for name in [".kilo", ".kilocode", ".opencode"] {
        dirs.push(root.join(name));
        dirs.push(home.join(name));
    }
    dirs
}

fn scan_skills(dir: &Path, map: &mut BTreeMap<String, SkillInfo>) {
    for base in ["skill", "skills"] {
        visit(&dir.join(base), &mut |path| {
            if path.file_name().and_then(|value| value.to_str()) != Some("SKILL.md") {
                return;
            }
            let Some((data, content)) = frontmatter(path) else {
                return;
            };
            let Some(name) = data.get("name").and_then(Value::as_str) else {
                return;
            };
            let Some(description) = data.get("description").and_then(Value::as_str) else {
                return;
            };
            if data.get("disabled").and_then(Value::as_bool) == Some(true) {
                return;
            }
            map.insert(
                name.to_string(),
                SkillInfo {
                    name: name.to_string(),
                    description: description.to_string(),
                    location: path.to_string_lossy().to_string(),
                    content,
                },
            );
        });
    }
}

fn scan_commands(dir: &Path, map: &mut BTreeMap<String, CommandInfo>) {
    for base in ["command", "commands"] {
        visit(&dir.join(base), &mut |path| {
            if path.extension().and_then(|value| value.to_str()) != Some("md") {
                return;
            }
            let Some((data, content)) = frontmatter(path) else {
                return;
            };
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                return;
            };
            if data.get("disabled").and_then(Value::as_bool) == Some(true) {
                return;
            }
            let name = data
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(stem)
                .to_string();
            let template = data
                .get("template")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or_else(|| content.trim())
                .to_string();
            map.insert(
                name.clone(),
                CommandInfo {
                    name,
                    description: data
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    agent: data
                        .get("agent")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    model: data
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    source: "command".to_string(),
                    hints: hints(&template),
                    template,
                    subtask: data.get("subtask").and_then(Value::as_bool),
                },
            );
        });
    }
}

fn visit(dir: &Path, f: &mut impl FnMut(&Path)) {
    let Ok(items) = fs::read_dir(dir) else {
        return;
    };
    for item in items.flatten() {
        let path = item.path();
        if path.is_dir() {
            visit(&path, f);
            continue;
        }
        f(&path);
    }
}

fn frontmatter(path: &Path) -> Option<(BTreeMap<String, Value>, String)> {
    let text = fs::read_to_string(path).ok()?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    let rest = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))?;
    let (raw, body) = split_frontmatter(rest)?;
    let mut data = BTreeMap::new();
    let mut lines = raw.lines().peekable();
    while let Some(line) = lines.next() {
        let line = line.trim_end();
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        let item = if value == "|" || value == "|-" || value == ">" || value == ">-" {
            json!(block(&mut lines, value.starts_with('>')))
        } else {
            scalar(value)
        };
        data.insert(key.to_string(), item);
    }
    Some((data, body.to_string()))
}

fn split_frontmatter(rest: &str) -> Option<(&str, &str)> {
    for mark in ["\n---\n", "\r\n---\r\n", "\n---\r\n", "\r\n---\n"] {
        if let Some((raw, body)) = rest.split_once(mark) {
            return Some((raw, body));
        }
    }
    rest.strip_suffix("\n---")
        .or_else(|| rest.strip_suffix("\r\n---"))
        .map(|raw| (raw, ""))
}

fn scalar(value: &str) -> Value {
    let value = value.trim();
    if value.is_empty() || value == "null" || value == "~" {
        return Value::Null;
    }
    let lower = value.to_ascii_lowercase();
    if lower == "true" {
        return json!(true);
    }
    if lower == "false" {
        return json!(false);
    }
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        return json!(value[1..value.len().saturating_sub(1)].to_string());
    }
    json!(value)
}

fn block<'a>(lines: &mut std::iter::Peekable<std::str::Lines<'a>>, fold: bool) -> String {
    let mut out = Vec::new();
    while let Some(line) = lines.peek().copied() {
        if !line.starts_with(' ') && !line.starts_with('\t') {
            break;
        }
        let line = lines.next().unwrap_or_default();
        out.push(line.trim_start().to_string());
    }
    if fold {
        out.join(" ")
    } else {
        out.join("\n")
    }
}

fn hints(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    for idx in 1..=9 {
        let hint = format!("${idx}");
        if template.contains(&hint) {
            out.push(hint);
        }
    }
    if template.contains("$ARGUMENTS") {
        out.push("$ARGUMENTS".to_string());
    }
    out
}
