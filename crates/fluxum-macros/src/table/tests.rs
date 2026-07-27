//! Validation and codegen of `#[fluxum::table]`, probed on the expansion
//! functions directly (the trybuild UI suite pins the end-to-end
//! compile-fail rendering, but runs outside coverage instrumentation).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use quote::quote;

fn expand_ok(args: TokenStream, input: TokenStream) -> String {
    try_expand(args, input)
        .expect("expansion must succeed")
        .to_string()
}

fn expand_err(args: TokenStream, input: TokenStream) -> String {
    try_expand(args, input)
        .expect_err("expansion must fail")
        .to_string()
}

/// A minimal valid table body reused by argument-level tests.
fn simple_table() -> TokenStream {
    quote! {
        struct Task {
            #[primary_key]
            id: u64,
            title: String,
        }
    }
}

// -- entry point ----------------------------------------------------------

#[test]
fn expand_renders_failures_as_compile_error() {
    let out = expand(
        TokenStream::new(),
        quote!(
            struct Broken;
        ),
    )
    .to_string();
    assert!(out.contains("compile_error !"), "{out}");
    assert!(out.contains("named fields"), "{out}");
}

#[test]
fn minimal_table_expands_schema_and_registration() {
    let out = expand_ok(TokenStream::new(), simple_table());
    assert!(out.contains("TableSchema"), "{out}");
    assert!(out.contains("TableDef"), "{out}");
    assert!(out.contains("inventory :: submit"), "{out}");
    assert!(out.contains("from_values"), "{out}");
}

// -- struct shape -----------------------------------------------------------

#[test]
fn rejects_generic_structs_tuple_unit_and_empty_structs() {
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T<A> {
                #[primary_key]
                id: u64,
                a: A,
            }
        },
    );
    assert!(err.contains("generic structs (DM-001)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote!(
            struct T(u64);
        ),
    );
    assert!(err.contains("named fields (DM-001)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote!(
            struct T;
        ),
    );
    assert!(err.contains("named fields (DM-001)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote!(
            struct T {}
        ),
    );
    assert!(err.contains("at least one column (DM-001)"), "{err}");
}

// -- table arguments ----------------------------------------------------------

#[test]
fn access_arguments_expand_and_conflict() {
    let out = expand_ok(quote!(global), simple_table());
    assert!(out.contains("TableAccess :: Global"), "{out}");

    let out = expand_ok(quote!(ephemeral), simple_table());
    assert!(out.contains("TableAccess :: Ephemeral"), "{out}");

    let err = expand_err(quote!(public, private), simple_table());
    assert!(err.contains("at most one of"), "{err}");

    let err = expand_err(quote!(fancy), simple_table());
    assert!(err.contains("unknown #[fluxum::table] argument"), "{err}");
}

#[test]
fn table_level_primary_key_argument_is_validated() {
    let no_field_pk = quote! {
        struct T {
            id: u64,
            region: u32,
        }
    };
    let err = expand_err(quote!(primary_key()), no_field_pk.clone());
    assert!(err.contains("at least one column (DM-003)"), "{err}");

    let err = expand_err(
        quote!(primary_key(id), primary_key(region)),
        no_field_pk.clone(),
    );
    assert!(err.contains("duplicate `primary_key(...)`"), "{err}");

    let err = expand_err(quote!(primary_key(missing)), no_field_pk.clone());
    assert!(err.contains("unknown column `missing`"), "{err}");

    let err = expand_err(quote!(primary_key(id, id)), no_field_pk.clone());
    assert!(err.contains("lists column `id` twice"), "{err}");

    let err = expand_err(quote!(primary_key(title)), simple_table());
    assert!(err.contains("declare exactly one (DM-003)"), "{err}");

    let err = expand_err(TokenStream::new(), no_field_pk);
    assert!(err.contains("no primary key"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                a: u64,
                #[primary_key]
                b: u64,
            }
        },
    );
    assert!(err.contains("duplicate `#[primary_key]`"), "{err}");
}

#[test]
fn partition_by_is_validated() {
    let table = quote! {
        struct T {
            #[primary_key]
            id: u64,
            region: u32,
        }
    };
    let out = expand_ok(quote!(public, partition_by(region)), table.clone());
    assert!(
        out.contains("partition_by : :: core :: option :: Option :: Some"),
        "{out}"
    );

    let err = expand_err(
        quote!(partition_by(region), partition_by(region)),
        table.clone(),
    );
    assert!(err.contains("duplicate `partition_by(...)`"), "{err}");

    let err = expand_err(quote!(global, partition_by(region)), table);
    assert!(err.contains("cannot be combined with `global`"), "{err}");
}

#[test]
fn expire_after_argument_is_validated() {
    let err = expand_err(quote!(expire_after = "1s"), simple_table());
    assert!(err.contains("only valid on an `ephemeral` table"), "{err}");

    let err = expand_err(
        quote!(ephemeral, expire_after = "1s", expire_after = "2s"),
        simple_table(),
    );
    assert!(err.contains("duplicate `expire_after`"), "{err}");

    let err = expand_err(quote!(ephemeral, expire_after = 5), simple_table());
    assert!(err.contains("must be a duration string"), "{err}");

    let out = expand_ok(quote!(ephemeral, expire_after = "10s"), simple_table());
    assert!(out.contains("EphemeralDef"), "{out}");
    assert!(out.contains("Some (10000000i64)"), "{out}");
    assert!(
        out.contains("owner : :: core :: option :: Option :: None"),
        "{out}"
    );
}

#[test]
fn duration_parsing_accepts_all_units_and_rejects_bad_input() {
    let span = Span::call_site();
    assert_eq!(parse_duration_us("500ms", span).unwrap(), 500_000);
    assert_eq!(parse_duration_us("10s", span).unwrap(), 10_000_000);
    assert_eq!(parse_duration_us("5m", span).unwrap(), 300_000_000);
    assert_eq!(parse_duration_us("2h", span).unwrap(), 7_200_000_000);

    for bad in ["10", "abc", "10d", "9223372036854775807h"] {
        let err = parse_duration_us(bad, span).unwrap_err().to_string();
        assert!(err.contains("invalid duration"), "{bad}: {err}");
    }
    let err = parse_duration_us("0s", span).unwrap_err().to_string();
    assert!(err.contains("positive duration"), "{err}");
}

// -- #[owner] / ephemeral metadata (SPEC-023 DMX-011) -----------------------

#[test]
fn owner_column_is_validated_and_registered() {
    let owned = quote! {
        struct Presence {
            #[primary_key]
            id: u64,
            #[owner]
            conn: ConnectionId,
        }
    };
    let out = expand_ok(quote!(ephemeral), owned.clone());
    assert!(out.contains("EphemeralDef"), "{out}");
    assert!(
        out.contains("owner : :: core :: option :: Option :: Some (1u16)"),
        "{out}"
    );
    assert!(
        out.contains("expire_after_us : :: core :: option :: Option :: None"),
        "{out}"
    );

    let out = expand_ok(quote!(ephemeral, expire_after = "500ms"), owned.clone());
    assert!(out.contains("Some (1u16)"), "{out}");
    assert!(out.contains("Some (500000i64)"), "{out}");

    let err = expand_err(TokenStream::new(), owned);
    assert!(err.contains("only valid on an `ephemeral` table"), "{err}");

    let err = expand_err(
        quote!(ephemeral),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[owner]
                a: ConnectionId,
                #[owner]
                b: ConnectionId,
            }
        },
    );
    assert!(err.contains("at most one `#[owner]`"), "{err}");

    let err = expand_err(
        quote!(ephemeral),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[owner]
                conn: u32,
            }
        },
    );
    assert!(err.contains("must be of type `ConnectionId`"), "{err}");
}

// -- unique / auto_inc --------------------------------------------------------

#[test]
fn unique_constraints_are_validated() {
    let out = expand_ok(
        TokenStream::new(),
        quote! {
            #[unique(title)]
            struct T {
                #[primary_key]
                id: u64,
                title: String,
            }
        },
    );
    assert!(out.contains("unique : & [& [1u16]]"), "{out}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[unique()]
            struct T {
                #[primary_key]
                id: u64,
            }
        },
    );
    assert!(err.contains("at least one column (DM-006)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[unique(missing)]
            struct T {
                #[primary_key]
                id: u64,
            }
        },
    );
    assert!(err.contains("unknown column `missing`"), "{err}");
}

#[test]
fn auto_inc_is_validated() {
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                #[auto_inc]
                a: u64,
                #[auto_inc]
                b: u64,
            }
        },
    );
    assert!(err.contains("duplicate `#[auto_inc]`"), "{err}");

    let err = expand_err(
        quote!(primary_key(a, b)),
        quote! {
            struct T {
                #[auto_inc]
                a: u64,
                b: u64,
            }
        },
    );
    assert!(err.contains("composite primary keys"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[auto_inc]
                n: u64,
            }
        },
    );
    assert!(
        err.contains("only valid on the `#[primary_key]` field"),
        "{err}"
    );

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                #[auto_inc]
                id: u32,
            }
        },
    );
    assert!(err.contains("to be `u64` (DM-004)"), "{err}");
}

// -- indexes --------------------------------------------------------------------

#[test]
fn btree_index_declarations_are_validated() {
    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[index(hash(title))]
            struct T {
                #[primary_key]
                id: u64,
                title: String,
            }
        },
    );
    assert!(
        err.contains("expected `#[index(btree(col, ...))]`"),
        "{err}"
    );

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[index(btree())]
            struct T {
                #[primary_key]
                id: u64,
            }
        },
    );
    assert!(
        err.contains("`btree(...)` needs at least one column"),
        "{err}"
    );

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[index(btree(price))]
            struct T {
                #[primary_key]
                id: u64,
                price: Decimal,
            }
        },
    );
    assert!(err.contains("cannot yet be a B-tree index key"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[index(btree(title))]
            #[index(btree(title))]
            struct T {
                #[primary_key]
                id: u64,
                title: String,
            }
        },
    );
    assert!(err.contains("duplicate `btree` index"), "{err}");
}

#[test]
fn spatial_index_declarations_are_validated() {
    let out = expand_ok(
        TokenStream::new(),
        quote! {
            #[spatial(rtree(ax, ay, bx, by))]
            struct Zone {
                #[primary_key]
                id: u64,
                ax: f64,
                ay: f64,
                bx: f64,
                by: f64,
            }
        },
    );
    assert!(out.contains("SpatialKind :: RTree"), "{out}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[spatial(quadtree(x, y))]
            #[spatial(rtree(ax, ay, bx, by))]
            struct T {
                #[primary_key]
                id: u64,
                x: f32,
                y: f32,
                ax: f64,
                ay: f64,
                bx: f64,
                by: f64,
            }
        },
    );
    assert!(err.contains("both `quadtree` and `rtree`"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[spatial(quadtree(x, y))]
            struct T {
                #[primary_key]
                id: u64,
                x: u32,
                y: f32,
            }
        },
    );
    assert!(err.contains("must be `f32` or `f64` (DM-032)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[spatial(kdtree(x, y))]
            struct T {
                #[primary_key]
                id: u64,
                x: f32,
                y: f32,
            }
        },
    );
    assert!(
        err.contains("expected `#[spatial(quadtree(x, y))]`"),
        "{err}"
    );

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[spatial(quadtree(x))]
            struct T {
                #[primary_key]
                id: u64,
                x: f32,
            }
        },
    );
    assert!(err.contains("exactly 2 coordinate columns"), "{err}");
}

// -- visibility ------------------------------------------------------------------

#[test]
fn visibility_rules_expand_and_are_validated() {
    let base = |vis: TokenStream| {
        quote! {
            #[visibility(#vis)]
            struct T {
                #[primary_key]
                id: u64,
                who: Identity,
                name: String,
            }
        }
    };
    let out = expand_ok(TokenStream::new(), base(quote!(public_all)));
    assert!(out.contains("VisibilityRule :: PublicAll"), "{out}");

    let out = expand_ok(TokenStream::new(), base(quote!(shard_local)));
    assert!(out.contains("VisibilityRule :: ShardLocal"), "{out}");

    let out = expand_ok(TokenStream::new(), base(quote!(custom(my_filter))));
    assert!(
        out.contains("VisibilityRule :: Custom (\"my_filter\")"),
        "{out}"
    );

    let err = expand_err(TokenStream::new(), base(quote!(owner_only(name))));
    assert!(err.contains("must be of type `Identity` (DM-060)"), "{err}");

    let err = expand_err(TokenStream::new(), base(quote!(nope)));
    assert!(err.contains("(DM-061)"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            #[visibility(public_all)]
            #[visibility(shard_local)]
            struct T {
                #[primary_key]
                id: u64,
            }
        },
    );
    assert!(err.contains("duplicate #[visibility]"), "{err}");
}

// -- field attributes ---------------------------------------------------------------

#[test]
fn field_level_misuse_is_rejected() {
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[unique(title)]
                title: String,
            }
        },
    );
    assert!(err.contains("table-level attribute"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[default(1u32)]
                #[default(2u32)]
                n: u32,
            }
        },
    );
    assert!(err.contains("duplicate `#[default]`"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[default]
                n: u32,
            }
        },
    );
    assert!(err.contains("with the backfill value"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[rename(from = "a")]
                #[rename(from = "b")]
                n: u32,
            }
        },
    );
    assert!(err.contains("duplicate `#[rename]`"), "{err}");
}

#[test]
fn rename_from_is_validated() {
    let with_rename = |args: TokenStream| {
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[rename(#args)]
                n: u32,
            }
        }
    };
    // Malformed argument shapes all render the usage error.
    for bad in [
        quote!(from(x)),
        quote!(to = "old"),
        quote!(from = old),
        quote!(from = 2),
    ] {
        let err = expand_err(TokenStream::new(), with_rename(bad.clone()));
        assert!(
            err.contains("expected `#[rename(from = \"old_name\")]`"),
            "{bad}: {err}"
        );
    }
    let err = expand_err(TokenStream::new(), with_rename(quote!(from = "")));
    assert!(err.contains("non-empty column name"), "{err}");

    // Consistency: self-rename, still-declared source, duplicate source.
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[rename(from = "n")]
                n: u32,
            }
        },
    );
    assert!(err.contains("names the field itself"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[rename(from = "other")]
                n: u32,
                other: u32,
            }
        },
    );
    assert!(err.contains("still declared"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[rename(from = "old")]
                a: u32,
                #[rename(from = "old")]
                b: u32,
            }
        },
    );
    assert!(err.contains("two columns declare"), "{err}");
}

// -- rich types (SPEC-023) ------------------------------------------------------------

#[test]
fn derived_columns_expand_but_cannot_be_keys() {
    let out = expand_ok(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                status: TaskStatus,
            }
        },
    );
    assert!(out.contains("FluxTypeDef"), "{out}");

    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                status: TaskStatus,
            }
        },
    );
    assert!(err.contains("rich types support"), "{err}");
}

// -- column transforms (SPEC-017) ------------------------------------------------------

#[test]
fn duplicate_transform_families_are_rejected() {
    // The second attribute of each family drives the CT-002 duplicate
    // error (and TransformDecl::span for every variant).
    let pairs: Vec<(TokenStream, TokenStream)> = vec![
        (
            quote!(normalize(money, scale = 2)),
            quote!(normalize(money, scale = 2)),
        ),
        (
            quote!(normalize(money, scale = 2)),
            quote!(normalize(datetime)),
        ),
        (
            quote!(normalize(money, scale = 2)),
            quote!(normalize(string)),
        ),
        (
            quote!(encrypted(ecies, key = "a")),
            quote!(encrypted(ecies, key = "b")),
        ),
        (
            quote!(signed(ed25519, by = server)),
            quote!(signed(ed25519, by = server)),
        ),
        (quote!(masked(null)), quote!(masked(redact))),
        (
            quote!(column_grant(select = public)),
            quote!(column_grant(select = owner)),
        ),
    ];
    for (first, second) in pairs {
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[#first]
                    #[#second]
                    x: Decimal,
                }
            },
        );
        assert!(err.contains("(CT-002)"), "{first} + {second}: {err}");
    }
}

#[test]
fn transform_pipeline_expands_in_canonical_order() {
    let out = expand_ok(
        TokenStream::new(),
        quote! {
            struct Payment {
                #[primary_key]
                id: u64,
                #[normalize(money, scale = 2, currency = "USD")]
                amount: Decimal,
                #[normalize(money, scale = 4)]
                fee: Decimal,
                #[normalize(datetime)]
                at: Timestamp,
                #[normalize(string)]
                plain: String,
                #[normalize(string, form = nfkc, case = fold, trim = true)]
                folded: String,
                #[normalize(string, case = lower)]
                lowered: String,
                #[encrypted(ecies, key = "k1")]
                secret: String,
                author: Identity,
                #[signed(ed25519, by = server)]
                receipt: Vec<u8>,
                #[signed(ed25519, by = author)]
                note: String,
                #[masked(null)]
                m_null: String,
                #[masked(redact)]
                m_redact: String,
                #[masked(hash)]
                m_hash: String,
                #[encrypted(ecies, key = "k2")]
                #[masked(ciphertext)]
                m_cipher: String,
                #[column_grant(select = public)]
                g_public: String,
                #[column_grant(select = owner)]
                g_owner: String,
                #[column_grant(select = server_peer)]
                g_peer: String,
                #[column_grant(select = "auditor")]
                g_role: String,
            }
        },
    );
    for expected in [
        "ColumnTransformDef",
        "NormalizeMoney",
        "Some (\"USD\")",
        "NormalizeDatetime",
        "NormalizeString",
        "StringForm :: Nfc",
        "StringForm :: Nfkc",
        "CaseFold :: None",
        "CaseFold :: Fold",
        "CaseFold :: Lower",
        "CryptoScheme :: Ecies",
        "SignScheme :: Ed25519",
        "SignedBy :: Server",
        "SignedBy :: IdentityColumn",
        "MaskStrategy :: Null",
        "MaskStrategy :: Redact",
        "MaskStrategy :: Hash",
        "MaskStrategy :: Ciphertext",
        "GrantScope :: Public",
        "GrantScope :: Owner",
        "GrantScope :: ServerPeer",
        "GrantScope :: Role (\"auditor\")",
    ] {
        assert!(out.contains(expected), "missing {expected}: {out}");
    }
}

#[test]
fn transform_column_type_requirements_are_enforced() {
    let single = |attr: TokenStream, ty: TokenStream| {
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[#attr]
                x: #ty,
            }
        }
    };
    let err = expand_err(
        TokenStream::new(),
        single(quote!(normalize(money, scale = 2)), quote!(String)),
    );
    assert!(err.contains("to be `Decimal`"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        single(quote!(normalize(datetime)), quote!(String)),
    );
    assert!(err.contains("to be `Timestamp`"), "{err}");

    let err = expand_err(
        TokenStream::new(),
        single(quote!(normalize(string)), quote!(u64)),
    );
    assert!(err.contains("to be `String`"), "{err}");

    // CT-013: encrypted columns can never be part of a key/index.
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                #[encrypted(ecies, key = "k")]
                id: u64,
            }
        },
    );
    assert!(err.contains("(CT-013)"), "{err}");

    // CT-033: `by` must reference an Identity column.
    let err = expand_err(
        TokenStream::new(),
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                title: String,
                #[signed(ed25519, by = title)]
                doc: String,
            }
        },
    );
    assert!(err.contains("must reference an"), "{err}");

    // CT-041: ciphertext masking requires encryption on the same column.
    let err = expand_err(
        TokenStream::new(),
        single(quote!(masked(ciphertext)), quote!(String)),
    );
    assert!(err.contains("(CT-041)"), "{err}");
}

#[test]
fn normalize_attribute_arguments_are_validated() {
    let with_attr = |attr: TokenStream| {
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[#attr]
                x: Decimal,
            }
        }
    };
    let cases: Vec<(TokenStream, &str)> = vec![
        (quote!(normalize()), "unknown normalize kind"),
        (quote!(normalize(base64)), "unknown normalize kind `base64`"),
        (
            quote!(normalize(money, scale = "2")),
            "`scale` must be an integer literal",
        ),
        (
            quote!(normalize(money, scale = 2, currency = usd)),
            "`currency` must be a string literal",
        ),
        (
            quote!(normalize(money, scale = 2, foo = 1)),
            "unknown `#[normalize(money)]` argument",
        ),
        (quote!(normalize(money)), "requires `scale`"),
        (
            quote!(normalize(datetime, assume_tz = "utc")),
            "takes no further arguments",
        ),
        (
            quote!(normalize(string, form = weird)),
            "`form` must be `nfc` or `nfkc`",
        ),
        // A literal where an ident is expected (meta_value_ident -> None).
        (
            quote!(normalize(string, form = "nfc")),
            "`form` must be `nfc` or `nfkc`",
        ),
        (
            quote!(normalize(string, case = weird)),
            "`case` must be `fold`, `lower`, or `none`",
        ),
        (
            quote!(normalize(string, trim = "yes")),
            "`trim` must be `true` or `false`",
        ),
        (
            quote!(normalize(string, pad = 4)),
            "unknown `#[normalize(string)]` argument",
        ),
    ];
    for (attr, expected) in cases {
        let err = expand_err(TokenStream::new(), with_attr(attr.clone()));
        assert!(err.contains(expected), "{attr}: {err}");
    }
}

#[test]
fn encrypted_signed_masked_grant_arguments_are_validated() {
    let with_attr = |attr: TokenStream| {
        quote! {
            struct T {
                #[primary_key]
                id: u64,
                #[#attr]
                x: String,
            }
        }
    };
    let cases: Vec<(TokenStream, &str)> = vec![
        (
            quote!(encrypted(aes256, key = "k")),
            "unknown encryption scheme `aes256`",
        ),
        (quote!(encrypted()), "expected `#[encrypted(ecies"),
        (
            quote!(encrypted(ecies, key = 5)),
            "`key` must be a string literal",
        ),
        (
            quote!(encrypted(ecies, nonce = "n")),
            "unknown `#[encrypted]` argument",
        ),
        (quote!(encrypted(ecies)), "non-empty key name"),
        (quote!(encrypted(ecies, key = "")), "non-empty key name"),
        (
            quote!(signed(rsa, by = server)),
            "unknown signature scheme `rsa`",
        ),
        (quote!(signed()), "expected `#[signed(ed25519"),
        (
            quote!(signed(ed25519, by = "server")),
            "`by` must be `server`",
        ),
        (quote!(signed(ed25519, by = a::b)), "`by` must be `server`"),
        (
            quote!(signed(ed25519, via = server)),
            "unknown `#[signed]` argument",
        ),
        (quote!(signed(ed25519)), "requires `by`"),
        (quote!(masked(zero)), "unknown mask strategy `zero`"),
        (
            quote!(column_grant(insert = public)),
            "expected `#[column_grant(select",
        ),
        (quote!(column_grant(select = "")), "must be non-empty"),
        (
            quote!(column_grant(select = nobody)),
            "`select` must be `public`",
        ),
    ];
    for (attr, expected) in cases {
        let err = expand_err(TokenStream::new(), with_attr(attr.clone()));
        assert!(err.contains(expected), "{attr}: {err}");
    }
}

// -- type mapping ----------------------------------------------------------------------

#[test]
fn flux_type_universe_maps_and_rejects() {
    let bytes = parse_flux_type(&syn::parse_quote!(Vec<u8>)).unwrap();
    assert!(bytes.tokens().to_string().contains("Bytes"));

    let list = parse_flux_type(&syn::parse_quote!(Vec<i8>)).unwrap();
    assert!(list.tokens().to_string().contains("List"));

    let opt = parse_flux_type(&syn::parse_quote!(Option<i16>)).unwrap();
    assert!(opt.tokens().to_string().contains("Option"));

    let unsupported: Vec<Type> = vec![
        syn::parse_quote!((u32, u32)),
        syn::parse_quote!(<Foo as Bar>::Baz),
        syn::parse_quote!(u32<u8>),
        syn::parse_quote!(Vec),
        syn::parse_quote!(Option<u8, u16>),
        syn::parse_quote!(Option<'static>),
    ];
    for ty in unsupported {
        let err = parse_flux_type(&ty).err().expect("must fail").to_string();
        assert!(err.contains("unsupported column type"), "{err}");
    }

    let maps: [Type; 2] = [
        syn::parse_quote!(HashMap<String, u32>),
        syn::parse_quote!(BTreeMap<String, u32>),
    ];
    for ty in maps {
        let err = parse_flux_type(&ty).err().expect("must fail").to_string();
        assert!(
            err.contains("map types are not valid column types"),
            "{err}"
        );
    }

    // A path type with no segments is impossible to parse but the mapper
    // still rejects it defensively.
    let empty = Type::Path(syn::TypePath {
        qself: None,
        path: syn::Path {
            leading_colon: None,
            segments: syn::punctuated::Punctuated::new(),
        },
    });
    let err = parse_flux_type(&empty)
        .err()
        .expect("must fail")
        .to_string();
    assert!(err.contains("unsupported column type"), "{err}");
}

#[test]
fn row_value_conversions_cover_every_scalar_variant() {
    let cases = [
        (FluxTy::I8, "I8"),
        (FluxTy::I16, "I16"),
        (FluxTy::EntityId, "EntityId"),
    ];
    for (flux, variant) in cases {
        assert!(flux.tokens().to_string().contains(variant));
        assert!(to_row_value(&flux, quote!(v)).to_string().contains(variant));
        assert!(
            from_row_value(&flux, quote!(src), "t", "c")
                .to_string()
                .contains(variant)
        );
    }
}
