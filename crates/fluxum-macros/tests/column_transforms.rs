//! SPEC-017 column transforms, phase-1 surface (task
//! phase1_column-transforms-type-surface, items 1.1/1.7/1.8/1.9): the five
//! per-column attributes parse, register a link-time `ColumnTransformDef` in
//! canonical CT-011 order, and validate against the assembled schema — with
//! `#[encrypted]` declared but (by design) not yet executed.
#![allow(dead_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::transform::{
    CaseFold, ColumnTransformDef, GrantScope, MaskStrategy, SignedBy, StringForm,
    TransformDescriptor, column_transforms,
};
use fluxum_core::types::{Decimal, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[index(btree(at))]
pub struct Payment {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    #[normalize(money, scale = 2, currency = "USD")]
    pub amount: Decimal,
    #[normalize(datetime)]
    pub at: Timestamp,
    // Declared out of canonical order on purpose: trim/case/form parse in any
    // order, and the emitted pipeline is still normalize→encrypted→…
    #[normalize(string, case = fold, trim = true, form = nfkc)]
    pub memo: String,
    // Grant + mask declared BEFORE encrypted: emission must reorder (CT-011).
    #[column_grant(select = server_peer)]
    #[masked(ciphertext)]
    #[encrypted(ecies, key = "payment_key")]
    pub card: Vec<u8>,
    #[signed(ed25519, by = owner)]
    pub total: i64,
}

fn payment_transforms(column: &str) -> &'static [TransformDescriptor] {
    column_transforms("Payment", column).unwrap_or_else(|| panic!("no transforms on {column}"))
}

#[test]
fn attributes_register_descriptors_with_the_declared_parameters() {
    assert_eq!(
        payment_transforms("amount"),
        &[TransformDescriptor::NormalizeMoney {
            scale: 2,
            currency: Some("USD"),
        }]
    );
    assert_eq!(
        payment_transforms("at"),
        &[TransformDescriptor::NormalizeDatetime]
    );
    assert_eq!(
        payment_transforms("memo"),
        &[TransformDescriptor::NormalizeString {
            form: StringForm::Nfkc,
            case: CaseFold::Fold,
            trim: true,
        }]
    );
    assert_eq!(
        payment_transforms("total"),
        &[TransformDescriptor::Signed {
            scheme: fluxum_core::transform::SignScheme::Ed25519,
            by: SignedBy::IdentityColumn(1), // `owner`
        }]
    );
    // Untransformed columns register nothing.
    assert!(column_transforms("Payment", "id").is_none());
    assert!(column_transforms("Payment", "owner").is_none());
}

#[test]
fn pipeline_is_emitted_in_canonical_ct011_order() {
    // Declared grant → masked → encrypted; stored encrypted → masked → grant.
    let card = payment_transforms("card");
    assert_eq!(card.len(), 3);
    assert!(matches!(
        card[0],
        TransformDescriptor::Encrypted {
            key: "payment_key",
            ..
        }
    ));
    assert!(matches!(
        card[1],
        TransformDescriptor::Masked {
            strategy: MaskStrategy::Ciphertext,
        }
    ));
    assert!(matches!(
        card[2],
        TransformDescriptor::Grant {
            select: GrantScope::ServerPeer,
        }
    ));
}

#[test]
fn assembled_schema_passes_transform_validation() {
    // CT-051 runtime backstop accepts the macro-emitted defs.
    let schema = Schema::from_tables([<Payment as Table>::SCHEMA]).unwrap();
    assert!(schema.table("Payment").is_some());
}

// --- CT-051 negative: a hand-registered def that violates the type rules ------

static MISMATCH_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "x",
    ty: FluxType::Str,
}];
static MISMATCH: TableSchema = TableSchema {
    name: "TransformMismatch",
    columns: MISMATCH_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "TransformMismatch",
        column: "x",
        transforms: &[TransformDescriptor::NormalizeMoney { scale: 2, currency: None }],
    }
}

#[test]
fn runtime_backstop_rejects_a_mistyped_hand_registered_def() {
    let err = match Schema::from_tables([&MISMATCH]) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("money-on-Str def must fail validation"),
    };
    assert!(err.contains("CT-021"), "{err}");
    // The def is scoped to its table: a schema without `TransformMismatch`
    // skips it (the registry is process-global, schemas may be subsets).
    assert!(Schema::from_tables([<Payment as Table>::SCHEMA]).is_ok());
}
