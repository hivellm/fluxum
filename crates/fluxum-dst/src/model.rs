//! The in-memory model oracle (TST-132): a trivially correct shadow of the
//! canonical two-table workload. The simulation drives the real engine and
//! this model with the same interaction stream and checks every observation
//! against it — acceptance/rejection parity per operation, full state
//! equality at every commit, and row-set equality after every crash-replay.

use std::collections::BTreeMap;

/// The committed logical state: `User` rows keyed by `id`, `Sensor` rows
/// keyed by `(grid_x, grid_y)` with the reading stored as raw `f64` bits
/// (exact equality, no float comparison semantics).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelState {
    /// `User(id, name)` rows.
    pub users: BTreeMap<u64, String>,
    /// `Sensor(grid_x, grid_y, reading)` rows.
    pub sensors: BTreeMap<(i32, i32), u64>,
}

impl ModelState {
    /// A canonical byte encoding for trace hashing.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (id, name) in &self.users {
            bytes.extend_from_slice(&id.to_le_bytes());
            bytes.extend_from_slice(name.as_bytes());
            bytes.push(0);
        }
        bytes.push(0xFF);
        for (&(x, y), bits) in &self.sensors {
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
            bytes.extend_from_slice(&bits.to_le_bytes());
        }
        bytes
    }
}

/// The model: current committed state plus the per-commit history needed to
/// rewind to any surviving prefix after a simulated crash (TST-133).
#[derive(Debug, Default)]
pub struct Model {
    /// `(tx_id, committed state after that tx)`, ascending.
    history: Vec<(u64, ModelState)>,
    current: ModelState,
}

impl Model {
    /// The committed state right now.
    pub fn current(&self) -> &ModelState {
        &self.current
    }

    /// Record a committed transaction: `state` is the full post-commit state.
    pub fn commit(&mut self, tx_id: u64, state: ModelState) {
        if let Some(&(last, _)) = self.history.last() {
            assert!(
                tx_id > last,
                "model commits must be ordered: {tx_id} after {last}"
            );
        }
        self.history.push((tx_id, state.clone()));
        self.current = state;
    }

    /// A simulated crash recovered prefix `1..=n`: forget everything after
    /// it. `n` must be 0 (empty state) or a committed tx id.
    pub fn retain_prefix(&mut self, n: u64) {
        while self.history.last().is_some_and(|&(tx, _)| tx > n) {
            self.history.pop();
        }
        self.current = self
            .history
            .last()
            .map(|(_, state)| state.clone())
            .unwrap_or_default();
        if n > 0 {
            let recovered = self.history.last().map_or(0, |&(tx, _)| tx);
            assert_eq!(
                recovered, n,
                "recovered prefix {n} is not a whole-transaction boundary (last model tx {recovered})"
            );
        }
    }

    /// Highest committed tx id the model knows.
    pub fn last_tx(&self) -> u64 {
        self.history.last().map_or(0, |&(tx, _)| tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_prefix_rewinds_exactly() {
        let mut model = Model::default();
        for tx in 1..=5u64 {
            let mut state = model.current().clone();
            state.users.insert(tx, format!("u{tx}"));
            model.commit(tx, state);
        }
        model.retain_prefix(3);
        assert_eq!(model.last_tx(), 3);
        assert_eq!(model.current().users.len(), 3);
        model.retain_prefix(0);
        assert_eq!(model.current(), &ModelState::default());
    }
}
