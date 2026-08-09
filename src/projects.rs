use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::RegisteredProject;

pub(crate) fn derive_project_label(cwd: Option<&str>) -> Option<String> {
    let cwd = cwd?;
    Path::new(cwd)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn slugify_project_token(value: &str) -> Option<String> {
    let mut slug = String::new();
    let mut last_was_sep = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if matches!(ch, '-' | '_' | ' ' | '.') && !last_was_sep {
            slug.push('-');
            last_was_sep = true;
        }
    }
    let slug = slug.trim_matches('-').to_string();
    (!slug.is_empty()).then_some(slug)
}

pub(crate) fn normalize_project_aliases(aliases: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = BTreeSet::new();
    for alias in aliases {
        if let Some(alias) = slugify_project_token(alias) {
            if seen.insert(alias.clone()) {
                normalized.push(alias);
            }
        }
    }
    normalized
}

pub(crate) fn ensure_unique_project_id(base: &str, existing_ids: &BTreeSet<String>) -> String {
    if !existing_ids.contains(base) {
        return base.to_string();
    }
    let mut index = 2_u64;
    loop {
        let candidate = format!("{base}-{index}");
        if !existing_ids.contains(&candidate) {
            return candidate;
        }
        index += 1;
    }
}

pub(crate) fn canonicalize_project_cwd(cwd: &str) -> Result<String> {
    let path = PathBuf::from(cwd.trim());
    if path.as_os_str().is_empty() {
        bail!("project cwd cannot be empty");
    }
    if !path.is_absolute() {
        bail!("project cwd must be an absolute path");
    }
    match fs::canonicalize(&path) {
        Ok(resolved) => Ok(resolved.display().to_string()),
        Err(_) => Ok(path.display().to_string()),
    }
}

pub(crate) fn build_registered_project(
    cwd: &str,
    id: Option<&str>,
    label: Option<&str>,
    aliases: &[String],
    existing_projects: &[RegisteredProject],
) -> Result<RegisteredProject> {
    let cwd = canonicalize_project_cwd(cwd)?;
    let label = label
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| derive_project_label(Some(&cwd)))
        .context("could not derive project label from cwd; pass --label")?;
    let requested_id = id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| slugify_project_token(&label))
        .context("could not derive project id from label; pass --id")?;
    let normalized_id = slugify_project_token(&requested_id)
        .context("project id must contain letters or digits")?;
    if existing_projects
        .iter()
        .any(|project| project.id == normalized_id && project.cwd != cwd)
    {
        bail!("project id `{normalized_id}` is already in use");
    }
    let aliases = normalize_project_aliases(aliases)
        .into_iter()
        .filter(|alias| alias != &normalized_id)
        .collect::<Vec<_>>();
    Ok(RegisteredProject {
        id: normalized_id,
        label,
        cwd,
        aliases,
    })
}

pub(crate) fn resolve_project_query<'a>(
    projects: &'a [RegisteredProject],
    query: &str,
) -> Result<&'a RegisteredProject> {
    let query = query.trim();
    if query.is_empty() {
        bail!("project query cannot be empty");
    }
    let normalized = query.to_ascii_lowercase();
    let exact = projects
        .iter()
        .filter(|project| {
            project.id.eq_ignore_ascii_case(&normalized)
                || project
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(&normalized))
                || project.label.eq_ignore_ascii_case(query)
        })
        .collect::<Vec<_>>();
    if exact.len() == 1 {
        return Ok(exact[0]);
    }
    if exact.len() > 1 {
        bail!("project query `{query}` is ambiguous");
    }
    let prefix = projects
        .iter()
        .filter(|project| {
            project.id.starts_with(&normalized)
                || project
                    .aliases
                    .iter()
                    .any(|alias| alias.starts_with(&normalized))
                || project.label.to_ascii_lowercase().starts_with(&normalized)
        })
        .collect::<Vec<_>>();
    match prefix.as_slice() {
        [project] => Ok(*project),
        [] => bail!("project `{query}` was not found"),
        _ => bail!("project query `{query}` is ambiguous"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedNewThreadRequest<'a> {
    pub(crate) project: &'a RegisteredProject,
    pub(crate) prompt: Option<String>,
}

pub(crate) fn resolve_new_thread_request<'a>(
    projects: &'a [RegisteredProject],
    current_project: Option<&'a RegisteredProject>,
    raw_prompt: Option<&str>,
) -> Result<ResolvedNewThreadRequest<'a>> {
    let prompt = raw_prompt
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let (project, prompt) = match prompt {
        Some(prompt) => {
            if let Some((project_query, rest)) = prompt.split_once(':') {
                let project_query = project_query.trim();
                let rest = rest.trim();
                if !project_query.is_empty() {
                    if let Ok(project) = resolve_project_query(projects, project_query) {
                        return Ok(ResolvedNewThreadRequest {
                            project,
                            prompt: (!rest.is_empty()).then_some(rest.to_string()),
                        });
                    }
                }
            }
            match current_project.or_else(|| (projects.len() == 1).then_some(&projects[0])) {
                Some(project) => (project, Some(prompt)),
                None => bail!("No current project selected. Use /project <id> first."),
            }
        }
        None => match current_project.or_else(|| (projects.len() == 1).then_some(&projects[0])) {
            Some(project) => (project, None),
            None => bail!("No current project selected. Use /project <id> first."),
        },
    };
    Ok(ResolvedNewThreadRequest { project, prompt })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_query_matches_id_alias_and_label() {
        let projects = vec![
            RegisteredProject {
                id: "bridge".to_string(),
                label: "Codex Telegram Bridge".to_string(),
                cwd: "/Users/hanifcarroll/projects/tools/codex-telegram-bridge".to_string(),
                aliases: vec!["codex".to_string(), "telegram-bridge".to_string()],
            },
            RegisteredProject {
                id: "ui-exp".to_string(),
                label: "UI Experiment".to_string(),
                cwd: "/Users/hanifcarroll/projects/ui-experiment".to_string(),
                aliases: vec!["ui".to_string()],
            },
        ];

        assert_eq!(
            resolve_project_query(&projects, "bridge").expect("id").id,
            "bridge"
        );
        assert_eq!(
            resolve_project_query(&projects, "codex").expect("alias").id,
            "bridge"
        );
        assert_eq!(
            resolve_project_query(&projects, "UI Experiment")
                .expect("label")
                .id,
            "ui-exp"
        );
    }

    #[test]
    fn new_thread_request_uses_current_project_and_override() {
        let projects = vec![
            RegisteredProject {
                id: "bridge".to_string(),
                label: "Codex Telegram Bridge".to_string(),
                cwd: "/Users/hanifcarroll/projects/tools/codex-telegram-bridge".to_string(),
                aliases: vec!["codex".to_string()],
            },
            RegisteredProject {
                id: "ui-exp".to_string(),
                label: "UI Experiment".to_string(),
                cwd: "/Users/hanifcarroll/projects/ui-experiment".to_string(),
                aliases: vec!["ui".to_string()],
            },
        ];

        let current = resolve_project_query(&projects, "bridge").expect("current");
        let defaulted =
            resolve_new_thread_request(&projects, Some(current), Some("Fix Telegram UX"))
                .expect("default request");
        assert_eq!(defaulted.project.id, "bridge");
        assert_eq!(defaulted.prompt.as_deref(), Some("Fix Telegram UX"));

        let overridden =
            resolve_new_thread_request(&projects, Some(current), Some("ui: tighten hero spacing"))
                .expect("override request");
        assert_eq!(overridden.project.id, "ui-exp");
        assert_eq!(overridden.prompt.as_deref(), Some("tighten hero spacing"));
    }
}
