//! `fluxum dev` (SPEC-024 DEV-010): the edit-save-see loop.
//!
//! Watch the module crate, rebuild on change, restart the server, regenerate
//! SDK bindings, and stream the merged logs — one command from edit to
//! running code:
//!
//! 1. **Watch**: a dependency-free mtime poll over `Cargo.toml`,
//!    `config.yml` and `src/**` (`target/`, `data/`, `.git/` ignored),
//!    debounced until the tree is stable for one interval — a save burst
//!    triggers one rebuild, not five.
//! 2. **Rebuild**: `cargo build --message-format=json-render-diagnostics`;
//!    the artifact path comes from cargo's own JSON, never guessed.
//! 3. **Restart** (DEV-010's "snapshot + log replay"): the old server is
//!    killed and the new binary starts over the SAME data dir — recovery
//!    replays the checkpoint + commit log, so the data survives the edit.
//! 4. **Bindings**: regenerated from the fresh server's `/schema` once it
//!    answers `/health`, so the next client call compiles against the new
//!    code.
//! 5. **Logs**: the child inherits this console — its structured module +
//!    system lines ARE the merged stream (`fluxum logs -f` attaches the
//!    same tap remotely, DEV-012).
//!
//! **Failure surfacing** (1.8): a rebuild failure keeps the PREVIOUS server
//! running and the loop alive — cargo's diagnostics are already on the
//! console, the next save retries. A server that dies on boot is reported
//! and the loop keeps watching.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::CliError;
use crate::generate;

/// Options for [`dev_loop`].
#[derive(Debug, Clone)]
pub struct DevOptions {
    /// The module crate directory.
    pub path: PathBuf,
    /// `host:port` the server's HTTP (admin) side will listen on — where
    /// `/health` is polled and `/schema` is fetched for bindings.
    pub http: String,
    /// Where regenerated bindings land; `None` skips regeneration.
    pub bindings: Option<PathBuf>,
    /// Bindings language.
    pub lang: generate::Lang,
    /// Watch poll interval.
    pub poll: Duration,
    /// Extra environment for the spawned server (tests pin ports here).
    pub env: Vec<(String, String)>,
    /// Run one build→start→health→bindings cycle and return (tests; the
    /// real loop never stops on its own).
    pub once: bool,
    /// Use this binary instead of running `cargo build` — the test/CI hook:
    /// cargo-in-cargo deadlocks on the build-directory lock, so the smoke
    /// drives the cycle with the workspace's prebuilt server.
    pub prebuilt: Option<PathBuf>,
}

impl Default for DevOptions {
    fn default() -> Self {
        Self {
            path: PathBuf::from("."),
            http: "127.0.0.1:15800".to_owned(),
            bindings: None,
            lang: generate::Lang::Rust,
            poll: Duration::from_millis(500),
            env: Vec::new(),
            once: false,
            prebuilt: None,
        }
    }
}

/// A change fingerprint over the watched tree: `Cargo.toml`, `config.yml`
/// and everything under `src/`, hashed as (relative path, mtime, len).
/// Build outputs (`target/`), server data (`data/`) and `.git/` never
/// participate — a rebuild must not retrigger itself.
#[must_use]
pub fn fingerprint(dir: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    for name in ["Cargo.toml", "config.yml"] {
        hash_file(&dir.join(name), Path::new(name), &mut hasher);
    }
    hash_tree(&dir.join("src"), Path::new("src"), &mut hasher);
    hasher.finish()
}

fn hash_file(path: &Path, rel: &Path, hasher: &mut DefaultHasher) {
    if let Ok(meta) = std::fs::metadata(path) {
        rel.hash(hasher);
        meta.len().hash(hasher);
        if let Ok(mtime) = meta.modified()
            && let Ok(since) = mtime.duration_since(std::time::UNIX_EPOCH)
        {
            since.as_nanos().hash(hasher);
        }
    }
}

fn hash_tree(dir: &Path, rel: &Path, hasher: &mut DefaultHasher) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    // Sorted so the fingerprint is order-stable across platforms.
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        let rel = rel.join(&name);
        let path = entry.path();
        if path.is_dir() {
            if !matches!(name.to_string_lossy().as_ref(), "target" | "data" | ".git") {
                hash_tree(&path, &rel, hasher);
            }
        } else {
            hash_file(&path, &rel, hasher);
        }
    }
}

/// Run `cargo build` in `dir` and return the built binary's path, parsed
/// from cargo's own `compiler-artifact` JSON (diagnostics render to stderr
/// as usual, so build errors reach the console untouched).
pub fn cargo_build(dir: &Path) -> Result<PathBuf, String> {
    let output = Command::new("cargo")
        .args(["build", "--message-format=json-render-diagnostics"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("cannot run cargo: {e}"))?;
    if !output.status.success() {
        return Err("build failed — fix the errors above and save again".to_owned());
    }
    parse_artifact(&String::from_utf8_lossy(&output.stdout))
        .ok_or_else(|| "cargo reported success but no binary artifact".to_owned())
}

/// The last `compiler-artifact` line carrying an executable — the crate's
/// own binary (dependencies report `"executable": null`).
#[must_use]
pub fn parse_artifact(cargo_stdout: &str) -> Option<PathBuf> {
    cargo_stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|v| v.get("reason").and_then(|r| r.as_str()) == Some("compiler-artifact"))
        .filter_map(|v| {
            v.get("executable")
                .and_then(|e| e.as_str())
                .map(PathBuf::from)
        })
        .next_back()
}

/// The running dev server child. Killed on drop, so a dev-loop panic never
/// leaves an orphan holding the ports.
struct ServerChild(Child);

impl ServerChild {
    fn spawn(exe: &Path, dir: &Path, env: &[(String, String)]) -> Result<ServerChild, String> {
        let mut command = Command::new(exe);
        command.current_dir(dir);
        for (key, value) in env {
            command.env(key, value);
        }
        // Inherited stdio IS the merged module+system log stream (DEV-010).
        command
            .spawn()
            .map(ServerChild)
            .map_err(|e| format!("cannot start {}: {e}", exe.display()))
    }

    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    /// Whether the child already exited (a boot failure to surface).
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.0.try_wait().ok().flatten()
    }
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Poll `GET /health` until the server answers 200 or the deadline passes.
fn wait_health(http: &str, deadline: Duration) -> Result<(), String> {
    let end = std::time::Instant::now() + deadline;
    loop {
        if let Ok(document) = crate::fetch_path(http, "/health")
            && document.contains("\"status\"")
        {
            return Ok(());
        }
        if std::time::Instant::now() >= end {
            return Err(format!("no healthy /health on {http} within {deadline:?}"));
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Regenerate bindings from the running server's `/schema` (DEV-010 step 4).
fn regen_bindings(http: &str, lang: generate::Lang, out: &Path) -> Result<usize, String> {
    let document = generate::load_schema(http).map_err(|e| e.to_string())?;
    let files = generate::generate(lang, &document).map_err(|e| e.to_string())?;
    let written = generate::write_files(out, &files).map_err(|e| e.to_string())?;
    Ok(written.len())
}

/// One build→restart→health→bindings cycle. `server` holds the previous
/// child; on build failure it is LEFT RUNNING (1.8) and the error returned.
fn cycle(options: &DevOptions, server: &mut Option<ServerChild>) -> Result<(), String> {
    let exe = match &options.prebuilt {
        Some(exe) => exe.clone(),
        None => cargo_build(&options.path)?,
    };
    // Only after a good build does the old server go away: the window
    // without a server is the restart itself, never a failed compile.
    if let Some(old) = server.as_mut() {
        old.stop();
    }
    *server = None;
    let mut child = ServerChild::spawn(&exe, &options.path, &options.env)?;
    match wait_health(&options.http, Duration::from_secs(30)) {
        Ok(()) => {}
        Err(e) => {
            let status = child
                .exited()
                .map_or("still running".to_owned(), |s| s.to_string());
            child.stop();
            return Err(format!("{e} (server: {status})"));
        }
    }
    if let Some(out) = &options.bindings {
        match regen_bindings(&options.http, options.lang, out) {
            Ok(count) => eprintln!("[fluxum dev] {count} binding file(s) → {}", out.display()),
            // Stale bindings are worse silent than loud, but they must not
            // kill a healthy server: report and carry on.
            Err(e) => eprintln!("[fluxum dev] bindings regeneration failed: {e}"),
        }
    }
    *server = Some(child);
    Ok(())
}

/// The `fluxum dev` loop. Returns the process exit code.
pub fn dev_loop(options: &DevOptions) -> Result<(), CliError> {
    if !options.path.join("Cargo.toml").exists() {
        return Err(CliError::Response(format!(
            "{} is not a crate (no Cargo.toml) — `fluxum init` scaffolds one",
            options.path.display()
        )));
    }
    let mut server: Option<ServerChild> = None;
    eprintln!(
        "[fluxum dev] watching {} (rebuild on save; Ctrl-C stops)",
        options.path.display()
    );
    match cycle(options, &mut server) {
        Ok(()) => eprintln!("[fluxum dev] up — edit, save, see"),
        Err(e) => eprintln!("[fluxum dev] {e}"),
    }
    if options.once {
        return Ok(()); // the child drops (and dies) with `server`
    }

    let mut current = fingerprint(&options.path);
    loop {
        std::thread::sleep(options.poll);
        // A dead child is surfaced, not silently tolerated (1.8) — the next
        // save (or this one, if the fingerprint moved) brings it back.
        if let Some(child) = server.as_mut()
            && let Some(status) = child.exited()
        {
            eprintln!("[fluxum dev] server exited ({status}) — waiting for the next save");
            server = None;
        }
        let seen = fingerprint(&options.path);
        if seen == current {
            continue;
        }
        // Debounce: wait until the tree is stable for one interval, so an
        // editor's save burst triggers one rebuild.
        let mut stable = seen;
        loop {
            std::thread::sleep(options.poll);
            let again = fingerprint(&options.path);
            if again == stable {
                break;
            }
            stable = again;
        }
        current = stable;
        eprintln!("[fluxum dev] change detected — rebuilding…");
        match cycle(options, &mut server) {
            Ok(()) => eprintln!("[fluxum dev] reloaded — data survived via snapshot + log replay"),
            Err(e) => eprintln!("[fluxum dev] {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn fingerprints_track_source_changes_and_ignore_build_outputs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]").unwrap();
        std::fs::write(dir.path().join("src/main.rs"), "fn main() {}").unwrap();
        let before = fingerprint(dir.path());

        // Build outputs and server data do not participate.
        std::fs::create_dir_all(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/junk"), "x").unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/wal"), "x").unwrap();
        assert_eq!(fingerprint(dir.path()), before, "target/data are ignored");

        // A source edit flips it (content length changes even when mtime
        // granularity is coarse).
        std::fs::write(dir.path().join("src/main.rs"), "fn main() { /* edited */ }").unwrap();
        assert_ne!(fingerprint(dir.path()), before);
    }

    #[test]
    fn artifact_parsing_takes_the_last_executable() {
        let stdout = concat!(
            r#"{"reason":"compiler-artifact","executable":null}"#,
            "\n",
            r#"{"reason":"compiler-artifact","executable":"/t/dep-tool"}"#,
            "\n",
            r#"{"reason":"compiler-artifact","executable":"/t/my-app"}"#,
            "\n",
            r#"{"reason":"build-finished","success":true}"#,
            "\n",
        );
        assert_eq!(parse_artifact(stdout).unwrap(), PathBuf::from("/t/my-app"));
        assert!(parse_artifact("").is_none());
        assert!(parse_artifact("not json\n").is_none());
    }

    #[test]
    fn dev_refuses_a_directory_without_a_crate() {
        let dir = tempfile::tempdir().unwrap();
        let options = DevOptions {
            path: dir.path().to_path_buf(),
            once: true,
            ..DevOptions::default()
        };
        let err = dev_loop(&options).unwrap_err();
        assert!(err.to_string().contains("fluxum init"), "{err}");
    }
}
