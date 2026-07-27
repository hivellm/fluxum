use super::*;

/// A 32-byte secret so `validate()` accepts the production profile.
const SECRET: &str = "0123456789abcdef0123456789abcdef";

fn env_of<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |key| {
        pairs
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| (*v).to_owned())
    }
}

/// `storage.data_dir` is the placement knob: setting it alone must move
/// the commit log, checkpoints, cold-tier pages, and the archive with
/// it — not leave them next to the process working directory.
#[test]
fn data_dir_alone_roots_every_storage_subdirectory() {
    let env = env_of(&[
        ("FLUXUM_STORAGE_DATA_DIR", "/srv/fluxum"),
        ("FLUXUM_AUTH_SECRET", SECRET),
    ]);
    let cfg = Config::load_with(None, &env).unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(cfg.storage.commit_log_dir, PathBuf::from("/srv/fluxum/log"));
    assert_eq!(
        cfg.storage.checkpoint_dir,
        PathBuf::from("/srv/fluxum/checkpoints")
    );
    assert_eq!(cfg.storage.page_dir, PathBuf::from("/srv/fluxum/pages"));
    assert_eq!(
        cfg.replication.archive.dir,
        PathBuf::from("/srv/fluxum/archive")
    );
    // The provenance says *why* they moved, so `/health` can explain it.
    assert_eq!(cfg.source_of("storage.page_dir"), ValueSource::Derived);
    assert_eq!(cfg.source_of("storage.data_dir"), ValueSource::Env);
}

/// An explicitly configured sub-directory keeps its exact value, even
/// when `data_dir` points elsewhere.
#[test]
fn an_explicit_subdirectory_outranks_the_data_dir() {
    let env = env_of(&[
        ("FLUXUM_STORAGE_DATA_DIR", "/srv/fluxum"),
        ("FLUXUM_STORAGE_PAGE_DIR", "/mnt/nvme/pages"),
        ("FLUXUM_AUTH_SECRET", SECRET),
    ]);
    let cfg = Config::load_with(None, &env).unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(cfg.storage.page_dir, PathBuf::from("/mnt/nvme/pages"));
    assert_eq!(cfg.source_of("storage.page_dir"), ValueSource::Env);
    // …while the ones left alone still follow data_dir.
    assert_eq!(cfg.storage.commit_log_dir, PathBuf::from("/srv/fluxum/log"));
}

/// A hand-built `Config` (embedders, tests) has no provenance to
/// consult, so a sub-directory still holding its built-in default is
/// treated as unset — and resolution is idempotent.
#[test]
fn a_hand_built_config_resolves_and_is_idempotent() {
    let mut cfg = Config::default();
    cfg.storage.data_dir = PathBuf::from("/var/lib/fluxum");
    cfg.storage.checkpoint_dir = PathBuf::from("/backup/checkpoints");

    cfg.resolve_storage_dirs();
    assert_eq!(cfg.storage.page_dir, PathBuf::from("/var/lib/fluxum/pages"));
    // Explicitly set before resolution: kept verbatim.
    assert_eq!(
        cfg.storage.checkpoint_dir,
        PathBuf::from("/backup/checkpoints")
    );

    let once = cfg.storage.page_dir.clone();
    cfg.resolve_storage_dirs();
    assert_eq!(cfg.storage.page_dir, once, "resolution is idempotent");
}

/// The defaults themselves are unchanged for a config that sets nothing.
#[test]
fn untouched_defaults_stay_put() {
    let cfg = Config::default();
    assert_eq!(cfg.storage.page_dir, PathBuf::from("./data/pages"));
    let mut resolved = cfg.clone();
    resolved.resolve_storage_dirs();
    assert_eq!(resolved.storage.page_dir, PathBuf::from("./data/pages"));
}
