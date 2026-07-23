//! `fluxum init` (SPEC-024 DEV-011): scaffold a runnable Fluxum module —
//! schema + reducers + config + a client stub — that boots with one command.
//!
//! A Fluxum application is a **crate**: the scaffold is a binary crate whose
//! `main` links its own module (tables/reducers register through `inventory`
//! at link time) and boots the embedded server, exactly like the reference
//! `fluxum-server` binary does with the demo module. `cargo run` is the one
//! command; `fluxum dev` wraps it with the edit-save-see loop (DEV-010).
//!
//! Until the fluxum crates are published, the scaffold's dependencies point
//! at a checkout via `--fluxum-path` (the generated README says so too).

use std::path::Path;

use crate::CliError;

/// Options for [`scaffold`].
#[derive(Debug, Clone)]
pub struct InitOptions {
    /// Crate/module name; defaults to the target directory's name.
    pub name: Option<String>,
    /// Path to a Fluxum checkout for path dependencies.
    pub fluxum_path: Option<String>,
    /// Template to instantiate. Only `notes` exists today; the flag is the
    /// stable surface more templates land behind (DEV-011).
    pub template: String,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            name: None,
            fluxum_path: None,
            template: "notes".to_owned(),
        }
    }
}

/// Sanitize a directory name into a crate name: lowercase, `-` for anything
/// that is not alphanumeric, no leading digit, never empty.
#[must_use]
pub fn crate_name(raw: &str) -> String {
    let mut name: String = raw
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    while name.contains("--") {
        name = name.replace("--", "-");
    }
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        name = format!("fluxum-app-{name}");
    }
    name.trim_matches('-').to_owned()
}

/// Scaffold `template` into `dir` (created; must not already contain a
/// `Cargo.toml`). Returns the relative paths written, in write order.
pub fn scaffold(dir: &Path, options: &InitOptions) -> Result<Vec<String>, CliError> {
    if options.template != "notes" {
        return Err(CliError::Response(format!(
            "unknown template {:?} (available: notes)",
            options.template
        )));
    }
    if dir.join("Cargo.toml").exists() {
        return Err(CliError::Response(format!(
            "{} already contains a Cargo.toml — refusing to overwrite a crate",
            dir.display()
        )));
    }
    let name = options.name.clone().unwrap_or_else(|| {
        crate_name(
            dir.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
                .as_str(),
        )
    });

    let dependencies = match &options.fluxum_path {
        Some(path) => {
            let path = path.replace('\\', "/");
            let path = path.trim_end_matches('/');
            format!(
                "fluxum-core = {{ path = \"{path}/crates/fluxum-core\" }}\n\
                 fluxum-macros = {{ path = \"{path}/crates/fluxum-macros\" }}\n\
                 fluxum-server = {{ path = \"{path}/crates/fluxum-server\" }}"
            )
        }
        // The crates are not on crates.io yet (only fluxum-sdk will be):
        // emit the placeholder the README explains, so the scaffold is
        // honest rather than silently unresolvable.
        None => "# Point these at your Fluxum checkout (see README.md), or re-run\n\
                 # `fluxum init` with --fluxum-path <checkout>:\n\
                 fluxum-core = { path = \"../fluxum/crates/fluxum-core\" }\n\
                 fluxum-macros = { path = \"../fluxum/crates/fluxum-macros\" }\n\
                 fluxum-server = { path = \"../fluxum/crates/fluxum-server\" }"
            .to_owned(),
    };

    let files: Vec<(String, String)> = vec![
        (
            "Cargo.toml".to_owned(),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
                 publish = false\n\n[dependencies]\n{dependencies}\n\
                 tokio = {{ version = \"1\", features = [\"rt-multi-thread\", \"macros\", \"signal\"] }}\n\
                 tracing = \"0.1\"\n"
            ),
        ),
        ("src/main.rs".to_owned(), main_rs(&name)),
        ("config.yml".to_owned(), CONFIG_YML.to_owned()),
        (".gitignore".to_owned(), "/target\n/data\n".to_owned()),
        ("README.md".to_owned(), readme(&name)),
    ];

    let mut written = Vec::with_capacity(files.len());
    for (rel, content) in files {
        let path = dir.join(&rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, content.as_bytes())?;
        written.push(rel);
    }
    Ok(written)
}

/// The scaffold's `src/main.rs`: a small notes module + the boot main.
fn main_rs(name: &str) -> String {
    format!(
        r#"//! {name} — a Fluxum application.
//!
//! The module IS this crate: `#[fluxum::table]` / `#[fluxum::reducer]`
//! register at link time, and `main` boots the embedded Fluxum server with
//! whatever is linked. Edit a reducer, save, and `fluxum dev` restarts the
//! server with your data intact (snapshot + commit-log replay).

use fluxum_core::reducer::ReducerContext;
use fluxum_core::types::{{Identity, Timestamp}};
use fluxum_macros as fluxum;

/// A note. Public: every subscriber sees every note.
#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Note {{
    /// Server-assigned id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// Who wrote it.
    pub author: Identity,
    /// The note body.
    pub body: String,
    /// Server timestamp at commit.
    pub created_at: Timestamp,
}}

/// Add a note. The server assigns `id` and stamps `created_at`.
#[fluxum::reducer]
fn add_note(ctx: &ReducerContext, body: String) -> Result<(), String> {{
    if body.trim().is_empty() {{
        return Err("a note needs a body".to_owned());
    }}
    tracing::info!(target: "{name}", "note added");
    ctx.tx
        .insert(Note {{
            id: 0, // #[auto_inc]
            author: ctx.identity,
            body,
            created_at: ctx.timestamp,
        }})
        .map_err(|e| e.to_string())?;
    Ok(())
}}

/// Delete one of YOUR notes; deleting someone else's is refused.
#[fluxum::reducer]
fn delete_note(ctx: &ReducerContext, id: u64) -> Result<(), String> {{
    let note = ctx
        .tx
        .query_pk::<Note>(id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no note {{id}}"))?;
    if note.author != ctx.identity {{
        return Err("only the author can delete a note".to_owned());
    }}
    ctx.tx.delete::<Note>(id).map_err(|e| e.to_string())?;
    Ok(())
}}

fn main() -> std::process::ExitCode {{
    let config = match fluxum_core::config::Config::load(Some(std::path::Path::new("config.yml")))
    {{
        Ok(config) => config,
        Err(err) => {{
            eprintln!("configuration error: {{err}}");
            return std::process::ExitCode::FAILURE;
        }}
    }};
    let log_handle = fluxum_server::logging::init(&config.logging).ok();
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {{
        Ok(runtime) => runtime,
        Err(err) => {{
            eprintln!("cannot start the async runtime: {{err}}");
            return std::process::ExitCode::FAILURE;
        }}
    }};
    runtime.block_on(async move {{
        let server = match fluxum_server::boot::serve(config.clone()).await {{
            Ok(server) => server,
            Err(err) => {{
                eprintln!("startup failed: {{err}}");
                return std::process::ExitCode::FAILURE;
            }}
        }};
        server
            .ctx
            .install_config(Some("config.yml".into()), config, log_handle);
        println!(
            "{name} listening — HTTP {{}} (admin + /rpc), TCP {{}}",
            server.http.local_addr, server.tcp.local_addr
        );
        let _ = tokio::signal::ctrl_c().await;
        server.ctx.begin_drain();
        server.shutdown();
        std::process::ExitCode::SUCCESS
    }})
}}
"#
    )
}

/// The scaffold's `config.yml`: development profile on the documented ports.
const CONFIG_YML: &str = "\
profile: development

server:
  http_port: 15800
  tcp_port: 15801

storage:
  data_dir: ./data
  commit_log_dir: ./data/log

logging:
  level: info
  format: pretty
";

/// The scaffold's README.
fn readme(name: &str) -> String {
    format!(
        "# {name}\n\n\
         A [Fluxum](https://github.com/hivellm/fluxum) application: the module is this crate —\n\
         tables and reducers register at link time and `main` boots the embedded server.\n\n\
         ## Run\n\n\
         ```sh\ncargo run\n```\n\n\
         Or the edit-save-see loop (rebuild + restart with data intact + regenerated\n\
         bindings + streamed logs):\n\n\
         ```sh\nfluxum dev\n```\n\n\
         Then, from another terminal:\n\n\
         ```sh\n\
         # the module contract:\ncurl http://127.0.0.1:15800/schema\n\
         # call a reducer:\ncurl -X POST http://127.0.0.1:15800/reducer/add_note -d '[\"hello\"]'\n\
         # follow the logs:\nfluxum logs --server 127.0.0.1:15800 -f\n\
         ```\n\n\
         ## Dependencies\n\n\
         The fluxum crates are not published yet: `Cargo.toml` points at a checkout by\n\
         path. Pass `--fluxum-path <checkout>` to `fluxum init`, or edit the paths.\n"
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn crate_names_are_sanitized() {
        assert_eq!(crate_name("My App!"), "my-app");
        assert_eq!(crate_name("9lives"), "fluxum-app-9lives");
        assert_eq!(crate_name("__"), "fluxum-app");
        assert_eq!(crate_name("notes"), "notes");
    }

    #[test]
    fn scaffold_writes_a_runnable_crate_shape() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("my-notes");
        let options = InitOptions {
            fluxum_path: Some("E:\\checkout\\fluxum".to_owned()),
            ..InitOptions::default()
        };
        let written = scaffold(&target, &options).unwrap();
        assert_eq!(
            written,
            ["Cargo.toml", "src/main.rs", "config.yml", ".gitignore", "README.md"]
        );
        let manifest = std::fs::read_to_string(target.join("Cargo.toml")).unwrap();
        assert!(manifest.contains("name = \"my-notes\""));
        // Windows paths are normalized to forward slashes for TOML.
        assert!(manifest.contains("E:/checkout/fluxum/crates/fluxum-server"));
        let main = std::fs::read_to_string(target.join("src/main.rs")).unwrap();
        assert!(main.contains("#[fluxum::table(public)]"));
        assert!(main.contains("fluxum_server::boot::serve"));
        let config = std::fs::read_to_string(target.join("config.yml")).unwrap();
        assert!(config.contains("http_port: 15800"));
    }

    #[test]
    fn scaffold_refuses_an_existing_crate_and_unknown_templates() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]").unwrap();
        let err = scaffold(dir.path(), &InitOptions::default()).unwrap_err();
        assert!(err.to_string().contains("refusing"), "{err}");

        let fresh = tempfile::tempdir().unwrap();
        let options = InitOptions {
            template: "spaceships".to_owned(),
            ..InitOptions::default()
        };
        let err = scaffold(fresh.path(), &options).unwrap_err();
        assert!(err.to_string().contains("unknown template"), "{err}");
    }
}
