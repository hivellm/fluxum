//! SIMD kernels with runtime dispatch (SPEC-016 §5–§8, T2.10).
//!
//! One portable binary per OS/arch (HWA-030): all ISA specialization above
//! the platform baseline (x86-64-v1, aarch64 NEON) is selected at **runtime**,
//! once, into a table of per-kernel function pointers ([`Dispatch`], HWA-031).
//! Scalar reference implementations are the permanent behavioral oracle
//! (HWA-051): every accelerated variant must be bit-identical to its scalar
//! reference on every input (HWA-050). Parity is enforced by the proptest
//! suites in `tests/simd_parity.rs` (HWA-052), by the ISA-matrix CI workflow
//! (HWA-053), and by a boot-time known-answer self-check that falls back to
//! scalar on mismatch (HWA-055).
//!
//! Initial kernel catalogue (SPEC-016 §6):
//!
//! | Kernel | Accelerated variants | Used by |
//! |---|---|---|
//! | [`Dispatch::crc32c`] (CRC-32C, Castagnoli) | SSE4.2 hardware CRC (x86-64), CRC extension (aarch64) | commit-log entries, page checksums (SPEC-002) |
//! | [`Dispatch::hash64`] (xxHash64) | none yet — see [`hash64`](self) module note (HWA-060) | partition routing (SPEC-007), index hashing |
//! | [`Dispatch::eval_i64`] / [`Dispatch::eval_f64`] | AVX2 (x86-64), NEON (aarch64) | subscription filters, scans (SPEC-005) |
//!
//! The FluxBIN batch codec kernel (HWA-043) registers here when its consumer
//! (fan-out serialization / page materialization) lands; the SPEC-006
//! row-at-a-time codec remains its oracle. LZ4/zstd compression is delegated
//! to library SIMD (HWA-045), never hand-rolled here.
//!
//! Forced selection for debugging (HWA-032): config
//! `simd: auto | avx512 | avx2 | neon | scalar` (env `FLUXUM_SIMD`). A forced
//! tier applies to every kernel that implements it and every other kernel
//! falls back to scalar; forcing a tier the CPU does not support aborts boot.
//! AVX-512 is accepted (and validated against the CPU) but no kernel
//! implements it yet, so it currently resolves every kernel to scalar.

mod crc32c;
mod hash64;
mod predicate;

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::config::SimdMode;
use crate::error::{FluxumError, Result};

/// Raw CRC-32C state update: `(state, bytes) -> state`, over the raw
/// (pre-inverted) register; init/final inversion lives in the safe API.
type Crc32cFn = fn(u32, &[u8]) -> u32;
/// xxHash64: `(bytes, seed) -> hash`.
type Hash64Fn = fn(&[u8], u64) -> u64;
/// Batched `i64` predicate: `(op, values, rhs, bitmap)`.
type PredI64Fn = fn(PredOp, &[i64], i64, &mut [u64]);
/// Batched `f64` predicate: `(op, values, rhs, bitmap)`.
type PredF64Fn = fn(PredOp, &[f64], f64, &mut [u64]);

/// An ISA tier a kernel variant can target. Auto-selection order (HWA-031):
/// `AVX-512 → AVX2 → SSE4.2 → scalar` on x86-64, `NEON → scalar` on aarch64,
/// scalar everywhere else; a kernel may implement only a subset of tiers and
/// dispatch falls through to the next available one.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// AVX-512 (x86-64). Accepted by dispatch; no kernel implements it yet.
    Avx512,
    /// AVX2 (x86-64).
    Avx2,
    /// SSE4.2 (x86-64) — carries the hardware CRC-32C instruction.
    Sse42,
    /// NEON / AdvSIMD (aarch64); the CRC kernel additionally requires the
    /// aarch64 CRC extension at this tier.
    Neon,
    /// Portable scalar reference — the oracle, always available (HWA-051).
    Scalar,
}

impl Tier {
    /// Lowercase name used in the HWA-033 selection report.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Avx512 => "avx512",
            Self::Avx2 => "avx2",
            Self::Sse42 => "sse42",
            Self::Neon => "neon",
            Self::Scalar => "scalar",
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Runtime-detected CPU capabilities (CPUID on x86-64, auxval/HWCAP on
/// aarch64 Linux, via `std::arch::is_*_feature_detected!`). Detection runs
/// once at dispatch construction — never per call (HWA-031).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct CpuFeatures {
    /// AVX-512 Foundation (x86-64).
    pub avx512f: bool,
    /// AVX2 (x86-64).
    pub avx2: bool,
    /// SSE4.2, including the hardware CRC-32C instruction (x86-64).
    pub sse42: bool,
    /// NEON / AdvSIMD (aarch64 baseline).
    pub neon: bool,
    /// CRC32 extension (aarch64).
    pub crc: bool,
}

impl CpuFeatures {
    /// Probe the running CPU.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self {
                avx512f: std::arch::is_x86_feature_detected!("avx512f"),
                avx2: std::arch::is_x86_feature_detected!("avx2"),
                sse42: std::arch::is_x86_feature_detected!("sse4.2"),
                ..Self::default()
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self {
                neon: std::arch::is_aarch64_feature_detected!("neon"),
                crc: std::arch::is_aarch64_feature_detected!("crc"),
                ..Self::default()
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::default()
        }
    }

    /// Whether `tier` can execute on this CPU (`Scalar` always can).
    pub fn supports(&self, tier: Tier) -> bool {
        match tier {
            Tier::Avx512 => self.avx512f,
            Tier::Avx2 => self.avx2,
            Tier::Sse42 => self.sse42,
            Tier::Neon => self.neon,
            Tier::Scalar => true,
        }
    }

    /// Human-readable list of the supported accelerated tiers, for errors.
    fn supported_names(&self) -> String {
        let names: Vec<&str> = [Tier::Avx512, Tier::Avx2, Tier::Sse42, Tier::Neon]
            .into_iter()
            .filter(|&t| self.supports(t))
            .map(Tier::as_str)
            .collect();
        if names.is_empty() {
            "none".to_owned()
        } else {
            names.join(", ")
        }
    }
}

/// The tier chosen for each kernel (HWA-033) — serialized into the
/// effective-configuration boot event and `GET /health`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Selection {
    /// CRC-32C kernel.
    pub crc32c: Tier,
    /// xxHash64 kernel.
    pub hash64: Tier,
    /// Batched `i64` predicate kernel.
    pub predicate_i64: Tier,
    /// Batched `f64` predicate kernel.
    pub predicate_f64: Tier,
}

impl Selection {
    /// `kernel=tier` report line for the boot log (HWA-033), e.g.
    /// `crc32c=sse42 hash64=scalar predicate_i64=avx2 predicate_f64=avx2`.
    pub fn report(&self) -> String {
        format!(
            "crc32c={} hash64={} predicate_i64={} predicate_f64={}",
            self.crc32c, self.hash64, self.predicate_i64, self.predicate_f64
        )
    }

    /// Every kernel on its scalar oracle.
    const fn scalar() -> Self {
        Self {
            crc32c: Tier::Scalar,
            hash64: Tier::Scalar,
            predicate_i64: Tier::Scalar,
            predicate_f64: Tier::Scalar,
        }
    }
}

/// A batched comparison predicate (SPEC-016 §6, HWA-044).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PredOp {
    /// `value == rhs` (IEEE for floats: NaN is never equal; `0.0 == -0.0`).
    Eq,
    /// `value < rhs` (IEEE ordered: false when either side is NaN).
    Lt,
    /// `value > rhs` (IEEE ordered: false when either side is NaN).
    Gt,
}

/// Number of `u64` words the selection bitmap for `n` rows needs.
pub const fn bitmap_words(n: usize) -> usize {
    n.div_ceil(64)
}

/// The tier the forced mode names, or `None` for `auto`.
fn forced_tier(mode: SimdMode) -> Option<Tier> {
    match mode {
        SimdMode::Auto => None,
        SimdMode::Avx512 => Some(Tier::Avx512),
        SimdMode::Avx2 => Some(Tier::Avx2),
        SimdMode::Neon => Some(Tier::Neon),
        SimdMode::Scalar => Some(Tier::Scalar),
    }
}

/// The tiers dispatch may consider, best first (HWA-031/HWA-032). Errs when
/// a forced tier is not supported by the CPU — fail fast, this is a
/// debugging knob and a silent fallback would mask the thing being bisected.
fn allowed_tiers(mode: SimdMode, features: &CpuFeatures) -> Result<Vec<Tier>> {
    match forced_tier(mode) {
        None => {
            let mut tiers: Vec<Tier> = [Tier::Avx512, Tier::Avx2, Tier::Sse42, Tier::Neon]
                .into_iter()
                .filter(|&t| features.supports(t))
                .collect();
            tiers.push(Tier::Scalar);
            Ok(tiers)
        }
        Some(Tier::Scalar) => Ok(vec![Tier::Scalar]),
        Some(tier) => {
            if !features.supports(tier) {
                return Err(FluxumError::config(format!(
                    "simd: forced tier `{tier}` is not supported by this CPU \
                     (supported accelerated tiers: {}); `simd: scalar` is always \
                     valid (HWA-032)",
                    features.supported_names()
                )));
            }
            Ok(vec![tier, Tier::Scalar])
        }
    }
}

/// First tier in `tiers` for which the kernel has a variant on this CPU.
fn pick(tiers: &[Tier], has_variant: impl Fn(Tier) -> bool) -> Tier {
    tiers
        .iter()
        .copied()
        .find(|&t| has_variant(t))
        .unwrap_or(Tier::Scalar)
}

/// Resolve the per-kernel tier selection for `mode` on a CPU with `features`
/// (HWA-031/HWA-032). Pure — unit-testable with synthetic feature sets; it
/// answers "what would be chosen" and never executes a kernel. The only
/// failure is forcing a tier the CPU does not support.
pub fn select(mode: SimdMode, features: &CpuFeatures) -> Result<Selection> {
    let tiers = allowed_tiers(mode, features)?;
    Ok(Selection {
        crc32c: pick(&tiers, |t| crc32c::variant(t, features).is_some()),
        hash64: pick(&tiers, |t| hash64::variant(t, features).is_some()),
        predicate_i64: pick(&tiers, |t| predicate::variant_i64(t, features).is_some()),
        predicate_f64: pick(&tiers, |t| predicate::variant_f64(t, features).is_some()),
    })
}

/// The per-kernel dispatch table: function pointers selected once at startup
/// (HWA-031), exposed as safe batch APIs so the indirect-call cost amortizes
/// over whole buffers and row batches (HWA-035, HWA-054).
#[derive(Clone, Copy, Debug)]
pub struct Dispatch {
    selection: Selection,
    crc32c: Crc32cFn,
    hash64: Hash64Fn,
    predicate_i64: PredI64Fn,
    predicate_f64: PredF64Fn,
}

impl Dispatch {
    /// Build a dispatch table for `mode` on the running CPU: runtime feature
    /// detection, tier selection (HWA-031/HWA-032), then the HWA-055
    /// known-answer self-check (a mismatching kernel falls back to scalar
    /// with an error log rather than serving divergent output).
    pub fn new(mode: SimdMode) -> Result<Self> {
        let features = CpuFeatures::detect();
        let selection = select(mode, &features)?;
        let mut dispatch = Self {
            selection,
            crc32c: crc32c::variant(selection.crc32c, &features).unwrap_or(crc32c::scalar_update),
            hash64: hash64::variant(selection.hash64, &features).unwrap_or(hash64::scalar),
            predicate_i64: predicate::variant_i64(selection.predicate_i64, &features)
                .unwrap_or(predicate::scalar_i64),
            predicate_f64: predicate::variant_f64(selection.predicate_f64, &features)
                .unwrap_or(predicate::scalar_f64),
        };
        dispatch.self_check();
        Ok(dispatch)
    }

    /// The per-kernel tier selection (HWA-033).
    pub fn selection(&self) -> Selection {
        self.selection
    }

    /// CRC-32C (Castagnoli, reflected polynomial `0x82F63B78` — the SPEC-002
    /// commit-log / page checksum, HWA-041) of `data`.
    pub fn crc32c(&self, data: &[u8]) -> u32 {
        self.crc32c_extend(0, data)
    }

    /// Continue a CRC-32C over more data:
    /// `crc32c_extend(crc32c(a), b) == crc32c(a ++ b)`.
    pub fn crc32c_extend(&self, crc: u32, data: &[u8]) -> u32 {
        !(self.crc32c)(!crc, data)
    }

    /// xxHash64 of `data` with `seed` — stable across every ISA variant,
    /// platform, and endianness (HWA-042); used for partition routing and
    /// index hashing.
    pub fn hash64(&self, data: &[u8], seed: u64) -> u64 {
        (self.hash64)(data, seed)
    }

    /// Evaluate `op` against `rhs` over an `i64` column, writing an
    /// LSB-first selection bitmap: bit `i % 64` of `out[i / 64]` is row `i`;
    /// bits past `values.len()` are zero. Bit-identical to row-at-a-time
    /// evaluation on every variant (HWA-044).
    ///
    /// # Panics
    /// If `out.len() != bitmap_words(values.len())`.
    pub fn eval_i64(&self, op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
        assert_eq!(
            out.len(),
            bitmap_words(values.len()),
            "selection bitmap must be exactly bitmap_words(values.len()) words"
        );
        (self.predicate_i64)(op, values, rhs, out);
    }

    /// [`Dispatch::eval_i64`] over an `f64` column. Float semantics are
    /// exactly Rust's scalar `==` / `<` / `>` (IEEE ordered): NaN never
    /// matches and `0.0 == -0.0` (HWA-044).
    ///
    /// # Panics
    /// If `out.len() != bitmap_words(values.len())`.
    pub fn eval_f64(&self, op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
        assert_eq!(
            out.len(),
            bitmap_words(values.len()),
            "selection bitmap must be exactly bitmap_words(values.len()) words"
        );
        (self.predicate_f64)(op, values, rhs, out);
    }

    /// HWA-055 boot-time known-answer self-check: run every selected
    /// non-scalar kernel against a fixed vector and compare with the scalar
    /// oracle; on mismatch, log an error and fall back to scalar for that
    /// kernel rather than serve with a divergent kernel. One-shot; no
    /// steady-state cost.
    fn self_check(&mut self) {
        if self.selection == Selection::scalar() {
            return;
        }
        // 131 elements: crosses the 128-lane boundary and every vector width.
        let bytes: Vec<u8> = (0u32..131)
            .map(|i| (i.wrapping_mul(37) >> 1) as u8)
            .collect();

        if self.selection.crc32c != Tier::Scalar
            && (self.crc32c)(!0, &bytes) != crc32c::scalar_update(!0, &bytes)
        {
            tracing::error!(
                target: "fluxum::simd",
                "crc32c `{}` failed the known-answer self-check; falling back to scalar (HWA-055)",
                self.selection.crc32c
            );
            self.crc32c = crc32c::scalar_update;
            self.selection.crc32c = Tier::Scalar;
        }

        if self.selection.hash64 != Tier::Scalar
            && (self.hash64)(&bytes, 0x9E37_79B9) != hash64::scalar(&bytes, 0x9E37_79B9)
        {
            tracing::error!(
                target: "fluxum::simd",
                "hash64 `{}` failed the known-answer self-check; falling back to scalar (HWA-055)",
                self.selection.hash64
            );
            self.hash64 = hash64::scalar;
            self.selection.hash64 = Tier::Scalar;
        }

        let ints: Vec<i64> = (0..131i64).map(|i| (i * 7919) % 257 - 128).collect();
        let floats: Vec<f64> = (0..131u32)
            .map(|i| match i % 9 {
                0 => f64::NAN,
                1 => -0.0,
                2 => 0.0,
                3 => f64::INFINITY,
                4 => f64::NEG_INFINITY,
                _ => (f64::from(i) - 65.0) * 0.5,
            })
            .collect();
        let words = bitmap_words(131);
        for op in [PredOp::Eq, PredOp::Lt, PredOp::Gt] {
            if self.selection.predicate_i64 != Tier::Scalar {
                let mut got = vec![0u64; words];
                let mut want = vec![0u64; words];
                (self.predicate_i64)(op, &ints, 3, &mut got);
                predicate::scalar_i64(op, &ints, 3, &mut want);
                if got != want {
                    tracing::error!(
                        target: "fluxum::simd",
                        "predicate_i64 `{}` failed the known-answer self-check; falling back to scalar (HWA-055)",
                        self.selection.predicate_i64
                    );
                    self.predicate_i64 = predicate::scalar_i64;
                    self.selection.predicate_i64 = Tier::Scalar;
                }
            }
            if self.selection.predicate_f64 != Tier::Scalar {
                let mut got = vec![0u64; words];
                let mut want = vec![0u64; words];
                (self.predicate_f64)(op, &floats, 0.5, &mut got);
                predicate::scalar_f64(op, &floats, 0.5, &mut want);
                if got != want {
                    tracing::error!(
                        target: "fluxum::simd",
                        "predicate_f64 `{}` failed the known-answer self-check; falling back to scalar (HWA-055)",
                        self.selection.predicate_f64
                    );
                    self.predicate_f64 = predicate::scalar_f64;
                    self.selection.predicate_f64 = Tier::Scalar;
                }
            }
        }
    }
}

static GLOBAL: OnceLock<Dispatch> = OnceLock::new();

/// Initialize the process-global dispatch table at boot (HWA-031). Validates
/// `mode` first — a forced tier the CPU does not support is a boot-abort
/// error even if the table already exists (HWA-032) — then logs the
/// per-kernel selection (HWA-033). Selection happens once per process
/// lifetime: if the table was already initialized, the existing one wins.
pub fn init_global(mode: SimdMode) -> Result<&'static Dispatch> {
    let dispatch = Dispatch::new(mode)?;
    let global = GLOBAL.get_or_init(|| dispatch);
    tracing::info!(
        target: "fluxum::simd",
        selection = %global.selection().report(),
        "simd dispatch selected"
    );
    Ok(global)
}

/// The process-global dispatch table. If [`init_global`] has not run yet,
/// self-initializes honoring the `FLUXUM_SIMD` env override so that
/// forced-scalar runs of the whole workspace suite genuinely force every
/// consumer (HWA-034).
///
/// # Panics
/// If `FLUXUM_SIMD` names an invalid mode or a tier this CPU does not
/// support — the debugging knob must fail loudly, never silently fall back
/// (HWA-032). Server boot resolves the mode through [`crate::config`] and
/// [`init_global`] instead, which surface the same condition as an error.
pub fn global() -> &'static Dispatch {
    GLOBAL.get_or_init(|| {
        let mode = match std::env::var("FLUXUM_SIMD") {
            Ok(raw) => match parse_mode(&raw) {
                Some(mode) => mode,
                None => {
                    panic!("FLUXUM_SIMD={raw} is not one of auto|avx512|avx2|neon|scalar (HWA-032)")
                }
            },
            Err(_) => SimdMode::Auto,
        };
        match Dispatch::new(mode) {
            Ok(dispatch) => dispatch,
            Err(e) => panic!("SIMD dispatch initialization failed: {e}"),
        }
    })
}

/// Parse a `FLUXUM_SIMD` value (config files go through serde instead).
fn parse_mode(raw: &str) -> Option<SimdMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "auto" => Some(SimdMode::Auto),
        "avx512" => Some(SimdMode::Avx512),
        "avx2" => Some(SimdMode::Avx2),
        "neon" => Some(SimdMode::Neon),
        "scalar" => Some(SimdMode::Scalar),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_words_rounds_up() {
        assert_eq!(bitmap_words(0), 0);
        assert_eq!(bitmap_words(1), 1);
        assert_eq!(bitmap_words(64), 1);
        assert_eq!(bitmap_words(65), 2);
        assert_eq!(bitmap_words(128), 2);
        assert_eq!(bitmap_words(129), 3);
    }

    #[test]
    fn scalar_mode_always_selects_the_oracle() {
        let sel = select(SimdMode::Scalar, &CpuFeatures::default()).unwrap();
        assert_eq!(sel, Selection::scalar());
        assert_eq!(
            sel.report(),
            "crc32c=scalar hash64=scalar predicate_i64=scalar predicate_f64=scalar"
        );
    }

    #[test]
    fn auto_without_features_selects_scalar() {
        let sel = select(SimdMode::Auto, &CpuFeatures::default()).unwrap();
        assert_eq!(sel, Selection::scalar());
    }

    #[test]
    fn forcing_an_unsupported_tier_is_a_boot_error() {
        let err = select(SimdMode::Avx2, &CpuFeatures::default()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("avx2"), "{msg}");
        assert!(msg.contains("HWA-032"), "{msg}");
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn auto_on_avx2_features_picks_per_kernel_tiers() {
        let feats = CpuFeatures {
            avx2: true,
            sse42: true,
            ..CpuFeatures::default()
        };
        let sel = select(SimdMode::Auto, &feats).unwrap();
        // The hardware CRC lives at the SSE4.2 tier; dispatch falls through
        // AVX-512 → AVX2 → SSE4.2 per kernel (HWA-031).
        assert_eq!(sel.crc32c, Tier::Sse42);
        // No accelerated hash variant has HWA-060 bench evidence yet.
        assert_eq!(sel.hash64, Tier::Scalar);
        assert_eq!(sel.predicate_i64, Tier::Avx2);
        assert_eq!(sel.predicate_f64, Tier::Avx2);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn forced_avx512_resolves_every_kernel_to_scalar_stub() {
        let feats = CpuFeatures {
            avx512f: true,
            avx2: true,
            sse42: true,
            ..CpuFeatures::default()
        };
        let sel = select(SimdMode::Avx512, &feats).unwrap();
        assert_eq!(sel, Selection::scalar(), "no kernel implements AVX-512 yet");
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn forced_avx2_falls_back_to_scalar_for_kernels_without_that_tier() {
        let feats = CpuFeatures {
            avx2: true,
            sse42: true,
            ..CpuFeatures::default()
        };
        let sel = select(SimdMode::Avx2, &feats).unwrap();
        // HWA-032: a kernel lacking the forced tier falls back to *scalar*,
        // not to a lower accelerated tier.
        assert_eq!(sel.crc32c, Tier::Scalar);
        assert_eq!(sel.predicate_i64, Tier::Avx2);
        assert_eq!(sel.predicate_f64, Tier::Avx2);
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn auto_on_neon_features_picks_per_kernel_tiers() {
        let feats = CpuFeatures {
            neon: true,
            crc: true,
            ..CpuFeatures::default()
        };
        let sel = select(SimdMode::Auto, &feats).unwrap();
        assert_eq!(sel.crc32c, Tier::Neon);
        assert_eq!(sel.hash64, Tier::Scalar);
        assert_eq!(sel.predicate_i64, Tier::Neon);
        assert_eq!(sel.predicate_f64, Tier::Neon);

        // Without the CRC extension the CRC kernel stays scalar even though
        // the NEON tier itself is available.
        let no_crc = CpuFeatures {
            neon: true,
            ..CpuFeatures::default()
        };
        assert_eq!(
            select(SimdMode::Auto, &no_crc).unwrap().crc32c,
            Tier::Scalar
        );
    }

    #[test]
    fn dispatch_builds_on_every_machine() {
        assert!(Dispatch::new(SimdMode::Scalar).is_ok());
        let auto = Dispatch::new(SimdMode::Auto).unwrap();
        // Whatever tier auto picked must produce the canonical CRC-32C.
        assert_eq!(auto.crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(auto.crc32c(b""), 0);
    }

    #[test]
    fn parse_mode_accepts_the_config_vocabulary() {
        assert_eq!(parse_mode("auto"), Some(SimdMode::Auto));
        assert_eq!(parse_mode("AVX512"), Some(SimdMode::Avx512));
        assert_eq!(parse_mode(" avx2 "), Some(SimdMode::Avx2));
        assert_eq!(parse_mode("neon"), Some(SimdMode::Neon));
        assert_eq!(parse_mode("scalar"), Some(SimdMode::Scalar));
        assert_eq!(parse_mode("sse42"), None, "SSE4.2 is not a forceable mode");
        assert_eq!(parse_mode(""), None);
    }

    #[test]
    fn global_returns_a_usable_dispatch() {
        let d = global();
        assert_eq!(d.crc32c(b""), 0);
    }
}
