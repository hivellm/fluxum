use super::reload::get_path;
use super::*;

/// Write a config file and return its path (kept alive by the dir).
/// Write a config file under the `development` profile (the default
/// `production` profile requires an auth secret, which is orthogonal to
/// what these tests are about).
fn write(dir: &tempfile::TempDir, text: &str) -> std::path::PathBuf {
    let path = dir.path().join("config.yml");
    std::fs::write(
        &path,
        format!(
            "profile: development
{text}"
        ),
    )
    .unwrap();
    path
}
fn no_env(_key: &str) -> Option<String> {
    None
}

#[test]
fn reloadable_keys_all_exist() {
    // An allowlist entry naming a key that does not exist would never
    // match, silently freezing the key it was meant to free.
    let value = serde_yaml::to_value(Config::default()).unwrap();
    for key in RELOADABLE_KEYS {
        let path: Vec<String> = key.split('.').map(str::to_owned).collect();
        assert!(
            get_path(&value, &path).is_some(),
            "RELOADABLE_KEYS names '{key}', which is not a real Config path"
        );
    }
}

#[test]
fn raising_the_log_level_is_accepted_and_reported() {
    let dir = tempfile::tempdir().unwrap();
    let running =
        Config::load_with(Some(&write(&dir, "logging:\n  level: info\n")), &no_env).unwrap();
    assert_eq!(running.logging.level, "info");

    // The operator raises verbosity and reloads (OPS-040).
    let path = write(&dir, "logging:\n  level: debug\n");
    let reload = running.reload_with(Some(&path), &no_env).unwrap();
    assert_eq!(reload.config.logging.level, "debug");
    assert_eq!(
        reload.changed,
        vec!["logging.level"],
        "exactly what changed, so the caller republishes only that"
    );
    // The running config is untouched — the new one only escapes in Ok.
    assert_eq!(running.logging.level, "info");
}

#[test]
fn a_changed_port_is_rejected_and_nothing_is_applied() {
    let dir = tempfile::tempdir().unwrap();
    let running =
        Config::load_with(Some(&write(&dir, "logging:\n  level: info\n")), &no_env).unwrap();
    let original_port = running.server.tcp_port;

    // A port change alongside a legitimately reloadable one: the whole
    // reload must fail, not partially apply the good half (OPS-041).
    let path = write(
        &dir,
        &format!(
            "logging:\n  level: debug\nserver:\n  tcp_port: {}\n",
            original_port + 1
        ),
    );
    let err = running.reload_with(Some(&path), &no_env).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("server.tcp_port"),
        "the error names the offending key: {message}"
    );
    assert!(
        message.contains("Restart to apply"),
        "and says what to do about it: {message}"
    );
    // Nothing applied: the running config kept BOTH values, including
    // the reloadable one that shared the rejected reload.
    assert_eq!(running.server.tcp_port, original_port);
    assert_eq!(running.logging.level, "info", "no partial apply");
}

#[test]
fn every_changed_non_reloadable_key_is_named_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let running = Config::load_with(Some(&write(&dir, "")), &no_env).unwrap();
    let path = write(&dir, "server:\n  tcp_port: 19999\nsharding:\n  shards: 8\n");
    let message = running
        .reload_with(Some(&path), &no_env)
        .unwrap_err()
        .to_string();
    // An operator fixing these one error at a time is a worse deploy.
    assert!(message.contains("server.tcp_port"), "{message}");
    assert!(message.contains("sharding.shards"), "{message}");
}

#[test]
fn an_unchanged_reload_is_a_no_op_success() {
    let dir = tempfile::tempdir().unwrap();
    let text = "logging:\n  level: warn\n";
    let running = Config::load_with(Some(&write(&dir, text)), &no_env).unwrap();
    // Re-reading identical config is a success with nothing to publish —
    // a SIGHUP with no edit must not be an error.
    let reload = running
        .reload_with(Some(&write(&dir, text)), &no_env)
        .unwrap();
    assert!(reload.changed.is_empty());
    assert_eq!(reload.config.logging.level, "warn");
}

#[test]
fn env_overrides_ride_the_reload_like_any_other_layer() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(&dir, "logging:\n  level: info\n");
    let running = Config::load_with(Some(&path), &no_env).unwrap();

    // OBS-080 precedence still holds on reload: env beats file.
    let with_env = |key: &str| -> Option<String> {
        (key == "FLUXUM_LOGGING_LEVEL").then(|| "trace".to_owned())
    };
    let reload = running.reload_with(Some(&path), &with_env).unwrap();
    assert_eq!(reload.config.logging.level, "trace");
    assert_eq!(reload.changed, vec!["logging.level"]);
}

#[test]
fn a_new_key_is_non_reloadable_until_someone_says_otherwise() {
    // The allowlist is the whole classification: anything absent from it
    // is frozen. This pins the fail-safe direction — the cost of
    // forgetting a key is a loud rejection, not a silent hot-swap.
    assert!(is_reloadable("logging.level"));
    assert!(!is_reloadable("storage.data_dir"));
    assert!(!is_reloadable("sharding.shards"));
    assert!(!is_reloadable("a.key.nobody.has.classified"));
}
