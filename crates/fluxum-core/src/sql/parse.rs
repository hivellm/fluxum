//! Recursive-descent parser for the SPEC-005 SQL subset (SUB-010..SUB-013).
//!
//! Grammar (keywords case-insensitive):
//!
//! ```text
//! query    := SELECT '*' FROM ident
//!             [ WHERE cond (AND cond)* ]
//!             [ IN REGION '(' num ',' num ',' num ',' num ')'
//!             | WITHIN RADIUS num OF '(' num ',' num ')' ]
//!             [ ORDER BY ident [ASC | DESC] ]
//!             [ LIMIT uint ]
//! cond     := ident '=' literal
//!           | ident IN '(' literal (',' literal)* ')'
//!           | ident BETWEEN literal AND literal
//! literal  := int | float | string | TRUE | FALSE
//! ```
//!
//! Everything outside the grammar — including every SUB-012 construct
//! (JOIN, GROUP BY/HAVING/aggregates, DML, subqueries, CTEs) — is rejected
//! with a wire-ready 400, with named diagnostics for the known constructs.

use crate::error::Result;

use super::lexer::{Token, unsupported};

/// A parsed literal, before schema-driven coercion.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Lit {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

impl std::fmt::Display for Lit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(x) => write!(f, "{x}"),
            Self::Str(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Self::Bool(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
        }
    }
}

/// One WHERE condition (SUB-010; comparison operators per SPEC-018 QP-030;
/// `MATCH` per SPEC-019 FTS-030).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CondAst {
    Eq(String, Lit),
    In(String, Vec<Lit>),
    Between(String, Lit, Lit),
    Cmp(String, CmpOp, Lit),
    /// `col MATCH 'raw query'` — analyzed at compile time (FTS-030).
    Match(String, String),
}

/// A QP-030 comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
}

impl std::fmt::Display for CmpOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        })
    }
}

/// One spatial clause (SUB-011).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SpatialAst {
    Region { x: f64, y: f64, w: f64, h: f64 },
    Radius { r: f64, x: f64, y: f64 },
}

/// The parsed query.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryAst {
    pub table: String,
    pub conditions: Vec<CondAst>,
    pub spatial: Option<SpatialAst>,
    pub order_by: Option<(String, bool)>, // (column, descending)
    /// An explicit second `ORDER BY` term (SPEC-018 QP-041): must name the
    /// primary key with the same direction — validated at compile, where
    /// the schema is in hand. The PK tiebreak is implicit when omitted.
    pub order_tiebreak: Option<(String, bool)>,
    pub limit: Option<u32>,
    /// `AFTER (order value, pk value)` keyset cursor (QP-040).
    pub after: Option<(Lit, Lit)>,
    /// `SELECT *, SCORE` — opt-in `_score` projection (FTS-041).
    pub select_score: bool,
    /// `AS OF TX <n>` / `AS OF TIMESTAMP <µs>` (SPEC-022 RV-021):
    /// `(is_timestamp, value)`.
    pub as_of: Option<(bool, i64)>,
}

/// SUB-012 constructs plus common SQL that is outside the subset, each with
/// a named diagnostic (the generic path catches everything else).
const REJECTED_KEYWORDS: &[(&str, &str)] = &[
    ("JOIN", "JOIN (use separate subscriptions per table)"),
    ("INNER", "JOIN (use separate subscriptions per table)"),
    ("OUTER", "JOIN (use separate subscriptions per table)"),
    ("LEFT", "JOIN (use separate subscriptions per table)"),
    ("RIGHT", "JOIN (use separate subscriptions per table)"),
    ("CROSS", "JOIN (use separate subscriptions per table)"),
    ("GROUP", "GROUP BY (use #[fluxum::view] for aggregates)"),
    ("HAVING", "HAVING (use #[fluxum::view] for aggregates)"),
    ("COUNT", "aggregate functions (use #[fluxum::view])"),
    ("SUM", "aggregate functions (use #[fluxum::view])"),
    ("AVG", "aggregate functions (use #[fluxum::view])"),
    ("MIN", "aggregate functions (use #[fluxum::view])"),
    ("MAX", "aggregate functions (use #[fluxum::view])"),
    ("INSERT", "INSERT (subscriptions are read-only)"),
    ("UPDATE", "UPDATE (subscriptions are read-only)"),
    ("DELETE", "DELETE (subscriptions are read-only)"),
    ("DROP", "DDL (subscriptions are read-only)"),
    ("ALTER", "DDL (subscriptions are read-only)"),
    ("CREATE", "DDL (subscriptions are read-only)"),
    ("WITH", "WITH (CTEs)"),
    ("UNION", "UNION"),
    ("OR", "OR (the subset combines conditions with AND only)"),
    ("NOT", "NOT"),
    ("NULL", "NULL comparisons"),
    ("IS", "IS (NULL comparisons)"),
    ("LIKE", "LIKE"),
];

fn reject_keyword(word: &str) -> Option<&'static str> {
    REJECTED_KEYWORDS
        .iter()
        .find(|(kw, _)| word.eq_ignore_ascii_case(kw))
        .map(|(_, msg)| *msg)
}

pub(crate) struct Parser<'t> {
    tokens: &'t [Token],
    pos: usize,
}

impl<'t> Parser<'t> {
    pub(crate) fn new(tokens: &'t [Token]) -> Self {
        Self { tokens, pos: 0 }
    }

    pub(crate) fn parse_query(mut self) -> Result<QueryAst> {
        self.expect_keyword("SELECT")?;
        if !matches!(self.next(), Some(Token::Star)) {
            return Err(unsupported(
                "subscriptions project whole rows: write SELECT *",
            ));
        }
        // FTS-041: `SELECT *, SCORE` opts into the `_score` projection.
        let mut select_score = false;
        if matches!(self.tokens.get(self.pos), Some(Token::Comma)) {
            self.pos += 1;
            self.expect_keyword("SCORE")?;
            select_score = true;
        }
        self.expect_keyword("FROM")?;
        let table = self.expect_ident("a table name")?;

        let mut conditions = Vec::new();
        if self.peek_keyword("WHERE") {
            self.pos += 1;
            loop {
                conditions.push(self.parse_condition()?);
                if self.peek_keyword("AND") {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        let mut spatial = None;
        if self.peek_keyword("IN") {
            self.pos += 1;
            self.expect_keyword("REGION")?;
            let [x, y, w, h] = self.parse_number_tuple::<4>()?;
            spatial = Some(SpatialAst::Region { x, y, w, h });
        } else if self.peek_keyword("WITHIN") {
            self.pos += 1;
            self.expect_keyword("RADIUS")?;
            let r = self.expect_number("the radius")?;
            self.expect_keyword("OF")?;
            let [x, y] = self.parse_number_tuple::<2>()?;
            spatial = Some(SpatialAst::Radius { r, x, y });
        }

        let mut order_by = None;
        let mut order_tiebreak = None;
        if self.peek_keyword("ORDER") {
            self.pos += 1;
            self.expect_keyword("BY")?;
            let (column, descending) = self.parse_order_term()?;
            order_by = Some((column, descending));
            // QP-041: at most one explicit tiebreak term (the primary key —
            // validated at compile time against the schema).
            if matches!(self.tokens.get(self.pos), Some(Token::Comma)) {
                self.pos += 1;
                order_tiebreak = Some(self.parse_order_term()?);
            }
        }

        let mut limit = None;
        if self.peek_keyword("LIMIT") {
            self.pos += 1;
            match self.next() {
                Some(Token::Int(n)) if *n >= 0 => {
                    limit =
                        Some(u32::try_from(*n).map_err(|_| {
                            unsupported(format!("LIMIT {n} exceeds the u32 range"))
                        })?);
                }
                other => {
                    return Err(unsupported(format!(
                        "LIMIT takes a non-negative integer, got {}",
                        display_token(other)
                    )));
                }
            }
        }

        // QP-040: the keyset cursor — `AFTER (order value, pk value)`.
        let mut after = None;
        if self.peek_keyword("AFTER") {
            self.pos += 1;
            if order_by.is_none() {
                return Err(unsupported(
                    "AFTER requires ORDER BY on an indexed column (QP-040)",
                ));
            }
            if !matches!(self.next(), Some(Token::LParen)) {
                return Err(unsupported("AFTER takes `(order value, pk value)`"));
            }
            let order_value = self.parse_literal()?;
            if !matches!(self.next(), Some(Token::Comma)) {
                return Err(unsupported("AFTER takes `(order value, pk value)`"));
            }
            let pk_value = self.parse_literal()?;
            if !matches!(self.next(), Some(Token::RParen)) {
                return Err(unsupported("expected `)` closing the AFTER cursor"));
            }
            after = Some((order_value, pk_value));
        } else if self.peek_keyword("OFFSET") {
            // QP-040: OFFSET is deliberately absent — linear and unstable
            // under concurrent writes; keyset is the sanctioned primitive.
            return Err(unsupported(
                "OFFSET (use keyset pagination: ORDER BY … LIMIT n AFTER (value, pk), QP-040)",
            ));
        }

        // SPEC-022 RV-021: `AS OF TX <n> | AS OF TIMESTAMP <µs>` — a
        // point-in-time read of the retained temporal window.
        let mut as_of = None;
        if self.peek_keyword("AS") {
            self.pos += 1;
            self.expect_keyword("OF")?;
            let is_timestamp = if self.peek_keyword("TX") {
                self.pos += 1;
                false
            } else if self.peek_keyword("TIMESTAMP") {
                self.pos += 1;
                true
            } else {
                return Err(unsupported(
                    "AS OF takes `TX <tx_id>` or `TIMESTAMP <µs since epoch>` (RV-021)",
                ));
            };
            match self.next() {
                Some(Token::Int(value)) if *value >= 0 => as_of = Some((is_timestamp, *value)),
                other => {
                    return Err(unsupported(format!(
                        "AS OF takes a non-negative integer, got {}",
                        display_token(other)
                    )));
                }
            }
        }

        if let Some(extra) = self.tokens.get(self.pos) {
            // A rejected construct after the parsed prefix (e.g. `... GROUP
            // BY c`, `... JOIN t`) gets its named SUB-012 diagnostic.
            if let Token::Word(word) = extra
                && let Some(named) = reject_keyword(word)
            {
                return Err(unsupported(named));
            }
            return Err(unsupported(format!(
                "unexpected trailing input starting at `{extra}`"
            )));
        }
        Ok(QueryAst {
            table,
            conditions,
            spatial,
            order_by,
            order_tiebreak,
            limit,
            after,
            select_score,
            as_of,
        })
    }

    /// One `ORDER BY` term: `ident [ASC | DESC]`.
    fn parse_order_term(&mut self) -> Result<(String, bool)> {
        let column = self.expect_ident("a column name")?;
        let mut descending = false;
        if self.peek_keyword("ASC") {
            self.pos += 1;
        } else if self.peek_keyword("DESC") {
            self.pos += 1;
            descending = true;
        }
        Ok((column, descending))
    }

    fn parse_condition(&mut self) -> Result<CondAst> {
        let column = self.expect_ident("a column name")?;
        match self.next() {
            Some(Token::Eq) => Ok(CondAst::Eq(column, self.parse_literal()?)),
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("IN") => {
                if !matches!(self.next(), Some(Token::LParen)) {
                    return Err(unsupported("IN takes a parenthesized value list"));
                }
                let mut values = vec![self.parse_literal()?];
                loop {
                    match self.next() {
                        Some(Token::Comma) => values.push(self.parse_literal()?),
                        Some(Token::RParen) => break,
                        other => {
                            return Err(unsupported(format!(
                                "expected `,` or `)` in the IN list, got {}",
                                display_token(other)
                            )));
                        }
                    }
                }
                Ok(CondAst::In(column, values))
            }
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("BETWEEN") => {
                let low = self.parse_literal()?;
                self.expect_keyword("AND")?;
                let high = self.parse_literal()?;
                Ok(CondAst::Between(column, low, high))
            }
            // SPEC-018 QP-030: the four comparison operators.
            Some(Token::Lt) => Ok(CondAst::Cmp(column, CmpOp::Lt, self.parse_literal()?)),
            Some(Token::Le) => Ok(CondAst::Cmp(column, CmpOp::Le, self.parse_literal()?)),
            Some(Token::Gt) => Ok(CondAst::Cmp(column, CmpOp::Gt, self.parse_literal()?)),
            Some(Token::Ge) => Ok(CondAst::Cmp(column, CmpOp::Ge, self.parse_literal()?)),
            // SPEC-019 FTS-030: full-text MATCH over a #[fulltext] column.
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("MATCH") => match self.next() {
                Some(Token::Str(query)) => Ok(CondAst::Match(column, query.clone())),
                other => Err(unsupported(format!(
                    "MATCH takes a quoted query string, got {}",
                    display_token(other)
                ))),
            },
            other => Err(unsupported(format!(
                "expected =, IN, BETWEEN, MATCH, <, >, <=, or >= after `{column}`, got {}",
                display_token(other)
            ))),
        }
    }

    fn parse_literal(&mut self) -> Result<Lit> {
        match self.next() {
            Some(Token::Int(n)) => Ok(Lit::Int(*n)),
            Some(Token::Float(x)) => Ok(Lit::Float(*x)),
            Some(Token::Str(s)) => Ok(Lit::Str(s.clone())),
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("TRUE") => Ok(Lit::Bool(true)),
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("FALSE") => Ok(Lit::Bool(false)),
            Some(Token::LParen) => Err(unsupported(
                "parenthesized expressions and subqueries are not allowed",
            )),
            other => Err(unsupported(format!(
                "expected a literal value, got {}",
                display_token(other)
            ))),
        }
    }

    /// `'(' num (',' num)* ')'` with exactly `N` numbers.
    fn parse_number_tuple<const N: usize>(&mut self) -> Result<[f64; N]> {
        if !matches!(self.next(), Some(Token::LParen)) {
            return Err(unsupported("expected `(` opening a coordinate list"));
        }
        let mut out = [0f64; N];
        for (index, slot) in out.iter_mut().enumerate() {
            if index > 0 && !matches!(self.next(), Some(Token::Comma)) {
                return Err(unsupported("expected `,` between coordinates"));
            }
            *slot = self.expect_number("a coordinate")?;
        }
        if !matches!(self.next(), Some(Token::RParen)) {
            return Err(unsupported("expected `)` closing the coordinate list"));
        }
        Ok(out)
    }

    fn expect_number(&mut self, what: &str) -> Result<f64> {
        match self.next() {
            Some(Token::Int(n)) => {
                #[allow(clippy::cast_precision_loss)] // spatial coordinates
                Ok(*n as f64)
            }
            Some(Token::Float(x)) => Ok(*x),
            other => Err(unsupported(format!(
                "expected {what} (a number), got {}",
                display_token(other)
            ))),
        }
    }

    /// Consume an identifier; a keyword from the rejected set gets its
    /// named SUB-012 diagnostic instead of a generic parse error.
    fn expect_ident(&mut self, what: &str) -> Result<String> {
        match self.next() {
            Some(Token::Word(w)) => {
                if let Some(named) = reject_keyword(w) {
                    return Err(unsupported(named));
                }
                Ok(w.clone())
            }
            other => Err(unsupported(format!(
                "expected {what}, got {}",
                display_token(other)
            ))),
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        match self.next() {
            Some(Token::Word(w)) if w.eq_ignore_ascii_case(keyword) => Ok(()),
            Some(Token::Word(w)) => {
                if let Some(named) = reject_keyword(w) {
                    return Err(unsupported(named));
                }
                Err(unsupported(format!("expected {keyword}, got `{w}`")))
            }
            other => Err(unsupported(format!(
                "expected {keyword}, got {}",
                display_token(other)
            ))),
        }
    }

    fn peek_keyword(&self, keyword: &str) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::Word(w)) if w.eq_ignore_ascii_case(keyword))
    }

    fn next(&mut self) -> Option<&'t Token> {
        let token = self.tokens.get(self.pos);
        self.pos += 1;
        token
    }
}

fn display_token(token: Option<&Token>) -> String {
    match token {
        Some(t) => format!("`{t}`"),
        None => "end of query".to_owned(),
    }
}
