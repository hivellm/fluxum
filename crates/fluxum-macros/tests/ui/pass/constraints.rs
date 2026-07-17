//! SPEC-022 RV-030/032: `#[check]`, `#[not_null]`, and `#[references]`
//! compile — including a self-referential FK and every referential action.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Node {
    #[primary_key]
    pub id: u64,
    /// Tree shape: a node references its parent node.
    #[references(Node(id), on_delete = cascade)]
    pub parent_id: Option<u64>,
    #[check(depth < 64)]
    #[check(depth > 0 || parent_id.is_none())]
    pub depth: u64,
    #[not_null]
    pub label: Option<String>,
}

#[fluxum::table(public)]
pub struct Leaf {
    #[primary_key]
    pub id: u64,
    #[references(Node(id))]
    pub node_id: u64,
    #[references(Node(id), on_delete = set_null)]
    pub shadow_node: Option<u64>,
}

fn main() {}
