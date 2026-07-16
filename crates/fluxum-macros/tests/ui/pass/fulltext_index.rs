//! SPEC-019 FTS-001: `#[fulltext(...)]` declarations compile — simple and
//! English analyzers, over `String`, `Option<String>`, and `Vec<String>`.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[fulltext(body)]
#[fulltext(title, english, stop_words, stemming)]
#[fulltext(tags, simple)]
pub struct Article {
    #[primary_key]
    pub id: u64,
    pub body: String,
    pub title: Option<String>,
    pub tags: Vec<String>,
}

fn main() {}
