//! `fluxum generate` (SPEC-011 SDK-010): emit typed client bindings from the
//! frozen `/schema` document.
//!
//! The schema can come from a running server (`--schema http://host:15800`)
//! or a file saved by `fluxum schema export` (`--schema ./schema.json`).
//! Both paths funnel through the same canonicalization, so **offline
//! generation and URL generation produce identical bytes** (SPEC-011
//! acceptance 11) — which is what lets a repository commit its bindings and
//! diff them in review.

pub mod typescript;

use std::collections::BTreeMap;
use std::path::Path;

use crate::CliError;

/// A target language for [`generate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lang {
    /// TypeScript (browser + Node), SDK-021.
    TypeScript,
}

impl Lang {
    /// Parse a `--lang` value.
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "typescript" | "ts" => Some(Self::TypeScript),
            _ => None,
        }
    }

    /// The `--lang` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TypeScript => "typescript",
        }
    }
}

/// Load the schema document from a URL or a file path.
///
/// Both produce the same canonical text: a URL is fetched and canonicalized
/// exactly as `fluxum schema export` does, and a file is re-canonicalized —
/// so a file that was *not* produced by the exporter still generates the same
/// bindings as its server would.
pub fn load_schema(source: &str) -> Result<serde_json::Value, CliError> {
    let canonical = if source.starts_with("http://")
        || source.starts_with("https://")
        || !Path::new(source).exists()
    {
        crate::fetch_schema(source)?
    } else {
        let raw = std::fs::read_to_string(source)?;
        crate::canonical_schema(&raw)?
    };
    serde_json::from_str(&canonical).map_err(|e| CliError::Response(e.to_string()))
}

/// Generate bindings for `lang` from `schema`, returning `filename ->
/// contents`. Pure: the same document always yields the same bytes.
pub fn generate(
    lang: Lang,
    schema: &serde_json::Value,
) -> Result<BTreeMap<String, String>, CliError> {
    match lang {
        Lang::TypeScript => typescript::generate(schema).map_err(CliError::Response),
    }
}

/// Write generated files into `out`, creating it if needed. Returns the paths
/// written, in order.
pub fn write_files(
    out: &Path,
    files: &BTreeMap<String, String>,
) -> Result<Vec<std::path::PathBuf>, CliError> {
    std::fs::create_dir_all(out)?;
    let mut written = Vec::with_capacity(files.len());
    for (name, contents) in files {
        let path = out.join(name);
        std::fs::write(&path, contents.as_bytes())?;
        written.push(path);
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn lang_parsing_accepts_the_documented_spellings() {
        assert_eq!(Lang::parse("typescript"), Some(Lang::TypeScript));
        assert_eq!(Lang::parse("TypeScript"), Some(Lang::TypeScript));
        assert_eq!(Lang::parse("ts"), Some(Lang::TypeScript));
        assert_eq!(Lang::parse("cobol"), None);
    }

    #[test]
    fn a_saved_schema_file_generates_what_its_server_would() {
        // SPEC-011 acceptance 11: offline == online. The file here is written
        // in a *different* key order and without pretty-printing; loading
        // re-canonicalizes, so the bindings match byte for byte.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        std::fs::write(
            &path,
            r#"{"reducers":[],"tables":[],"document_version":1,"schema_version":7}"#,
        )
        .unwrap();

        let from_file = load_schema(path.to_str().unwrap()).unwrap();
        let canonical = serde_json::json!({
            "schema_version": 7, "document_version": 1, "tables": [], "reducers": []
        });
        assert_eq!(
            generate(Lang::TypeScript, &from_file).unwrap(),
            generate(Lang::TypeScript, &canonical).unwrap()
        );
    }

    #[test]
    fn write_files_creates_the_output_directory() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("deep").join("gen");
        let mut files = BTreeMap::new();
        files.insert("a.ts".to_owned(), "export const a = 1;\n".to_owned());
        let written = write_files(&out, &files).unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(
            std::fs::read_to_string(out.join("a.ts")).unwrap(),
            "export const a = 1;\n"
        );
    }
}
