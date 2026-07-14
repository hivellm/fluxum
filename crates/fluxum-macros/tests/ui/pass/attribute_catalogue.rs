//! Every remaining DM-020 table attribute in one place: `private` (explicit),
//! `global`, `#[unique]`, composite btree, `rtree`, all visibility rules,
//! and the full §3 column type universe including `Option`/`Vec` nesting.

use fluxum_core::schema::{IndexSchema, SpatialKind, Table, TableAccess, VisibilityRule};
use fluxum_core::types::{EntityId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(private)]
#[unique(region, name)]
pub struct Zone {
    #[primary_key]
    pub id: EntityId,
    pub region: u32,
    pub name: String,
}

#[fluxum::table(global)]
pub struct SettingsEntry {
    #[primary_key]
    pub key: String,
    pub value: Vec<u8>,
    pub updated_at: Timestamp,
}

#[fluxum::table(public)]
#[spatial(rtree(min_x, min_y, max_x, max_y))]
#[visibility(shard_local)]
pub struct Region {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

#[fluxum::table(public)]
#[visibility(custom(sensor_filter))]
pub struct Reading {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub tags: Vec<String>,
    pub calibration: Option<f64>,
    pub history: Vec<Option<i64>>,
    pub flags: Option<Vec<u8>>,
    pub active: bool,
    pub small: i8,
    pub medium: i16,
    pub wide: i32,
    pub wider: i64,
    pub tiny: u8,
    pub short: u16,
    pub ratio: f32,
}

#[fluxum::table(public)]
#[visibility(public_all)]
pub struct Announcement {
    #[primary_key]
    pub id: u64,
    pub body: String,
}

fn main() {
    assert_eq!(Zone::SCHEMA.unique, &[&[1u16, 2][..]]);
    assert_eq!(Zone::SCHEMA.access, TableAccess::Private);
    assert_eq!(SettingsEntry::SCHEMA.access, TableAccess::Global);
    assert_eq!(
        Region::SCHEMA.indexes,
        &[IndexSchema::Spatial {
            kind: SpatialKind::RTree,
            columns: &[1, 2, 3, 4],
        }]
    );
    assert_eq!(Region::SCHEMA.visibility, VisibilityRule::ShardLocal);
    assert_eq!(
        Reading::SCHEMA.visibility,
        VisibilityRule::Custom("sensor_filter")
    );
    assert_eq!(
        Announcement::SCHEMA.visibility,
        VisibilityRule::PublicAll
    );
}
