//! Optional static file serving, for demos and bundled consoles.
//!
//! A database is not a web server, and this is **off unless configured**
//! (`server.static_dir`). It exists because the browser leaves no alternative:
//! `/rpc` sends no CORS headers, so a page that talks to Fluxum has to be
//! served from the same origin as `/rpc` itself. Hosting the page elsewhere
//! means the browser blocks every request before the SDK sees it.
//!
//! Because this turns a path in a URL into a path on disk, the only rule that
//! matters is that it cannot escape the configured root — see [`resolve`].

use std::path::{Component, Path, PathBuf};

/// Map a URL path to a file inside `root`, or `None` if it escapes.
///
/// Rejects anything that is not a plain name: `..` climbs out, a root
/// component (`/etc`, `C:\`) ignores the base entirely, and a prefix
/// (`C:`, `\\server\share`) does the same on Windows. Rather than sanitising
/// a string — where every encoding trick is a new bypass — this walks the
/// parsed components and accepts only `Normal` ones.
pub fn resolve(root: &Path, url_path: &str) -> Option<PathBuf> {
    let trimmed = url_path.split(['?', '#']).next().unwrap_or(url_path);
    let relative = trimmed.trim_start_matches('/');
    // A bare `/` (or a directory) means the index.
    let relative = if relative.is_empty() { "index.html" } else { relative };

    let mut out = root.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => out.push(part),
            // `.` is harmless but pointless; everything else escapes.
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    // A directory URL serves its index, so `/demo/` works like `/demo/index.html`.
    if out.is_dir() {
        out.push("index.html");
    }
    Some(out)
}

/// `Content-Type` for a file extension.
///
/// Deliberately a short closed list: guessing wrong on a script means the
/// browser refuses to execute it, and the failure looks like a broken page
/// rather than a wrong header.
pub fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn plain_paths_resolve_under_the_root() {
        let root = Path::new("/srv/demo");
        assert_eq!(resolve(root, "/app.js").unwrap(), root.join("app.js"));
        assert_eq!(
            resolve(root, "/assets/style.css").unwrap(),
            root.join("assets").join("style.css")
        );
    }

    #[test]
    fn the_bare_root_serves_the_index() {
        let root = Path::new("/srv/demo");
        assert_eq!(resolve(root, "/").unwrap(), root.join("index.html"));
        assert_eq!(resolve(root, "").unwrap(), root.join("index.html"));
    }

    #[test]
    fn a_query_string_is_not_part_of_the_filename() {
        let root = Path::new("/srv/demo");
        assert_eq!(resolve(root, "/app.js?v=2").unwrap(), root.join("app.js"));
        assert_eq!(resolve(root, "/app.js#top").unwrap(), root.join("app.js"));
    }

    #[test]
    fn traversal_is_refused() {
        // The whole point of the module. Each of these reads a file the
        // operator never meant to publish.
        let root = Path::new("/srv/demo");
        for attack in [
            "/../secret",
            "/../../etc/passwd",
            "/assets/../../secret",
            "/./../secret",
        ] {
            assert!(resolve(root, attack).is_none(), "{attack} must be refused");
        }
    }

    #[test]
    fn an_absolute_looking_path_stays_under_the_root() {
        // `Path::join` with an absolute path DISCARDS the base — the classic
        // way a "join under root" check turns out not to be one. Leading
        // slashes are stripped before the walk, so this lands *inside* the
        // root rather than at the filesystem root. Containment is the
        // property; rejection is not required.
        let root = Path::new("/srv/demo");
        assert_eq!(
            resolve(root, "//etc/passwd").unwrap(),
            root.join("etc").join("passwd")
        );
    }

    #[test]
    fn content_types_cover_what_a_module_page_needs() {
        assert_eq!(content_type(Path::new("i.html")), "text/html; charset=utf-8");
        assert_eq!(
            content_type(Path::new("a.js")),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type(Path::new("x.bin")), "application/octet-stream");
    }
}
