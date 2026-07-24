//! The shipped `config/config.example.yml` is the config reference of record
//! (SPEC-012 OBS-080; the deployment guide's "config reference" section
//! points at it): it must parse with the real strict deserializer, and it
//! must name EVERY key the `Config` struct has — so the reference cannot
//! silently drift when a new key lands.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::config::Config;

fn example_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/config.example.yml")
}

/// Collect every leaf key of a YAML mapping as a dotted path. A nested
/// mapping recurses; anything else (scalar, list, null) is a leaf.
fn leaf_keys(value: &serde_yaml::Value, prefix: &str, out: &mut Vec<String>) {
    if let serde_yaml::Value::Mapping(map) = value {
        for (key, child) in map {
            let name = key.as_str().unwrap_or_default();
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}.{name}")
            };
            match child {
                serde_yaml::Value::Mapping(_) => leaf_keys(child, &path, out),
                _ => out.push(path),
            }
        }
    }
}

#[test]
fn the_example_config_parses_with_the_strict_deserializer() {
    let text = std::fs::read_to_string(example_path()).unwrap();
    // Every struct is `deny_unknown_fields`, so a typo or a removed key in
    // the example fails here. (The full loader additionally validates
    // semantic constraints like auth.secret presence, which are deliberately
    // unmet by a reference file full of placeholder values.)
    let parsed: Config = serde_yaml::from_str(&text).expect("config.example.yml must parse");
    // The example documents the built-in defaults; spot-check the anchors
    // the deployment guide quotes.
    assert_eq!(parsed.server.http_port, 15800);
    assert_eq!(parsed.server.tcp_port, 15801);
}

#[test]
fn the_example_config_names_every_key() {
    let text = std::fs::read_to_string(example_path()).unwrap();
    let example: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
    let defaults = serde_yaml::to_value(Config::default()).unwrap();

    let mut expected = Vec::new();
    leaf_keys(&defaults, "", &mut expected);
    let mut present = Vec::new();
    leaf_keys(&example, "", &mut present);

    // `sources` is #[serde(skip)] and never appears on either side. A key in
    // the defaults but not the example means the reference has drifted.
    let missing: Vec<&String> = expected.iter().filter(|k| !present.contains(k)).collect();
    assert!(
        missing.is_empty(),
        "config/config.example.yml is missing these keys (add them with their \
         defaults + a comment): {missing:?}"
    );
}
