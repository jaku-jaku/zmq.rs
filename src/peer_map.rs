use crate::util::PeerIdentity;

use parking_lot::Mutex;

use std::collections::HashMap;

/// The peer table of one socket.
///
/// Every access takes a short sync lock inside one method call, and no guard
/// leaves the method, so the lock is never held across an `.await`, and no
/// async task ever waits for it. A thread that waits for the lock waits only
/// for another thread that is running, so the table is safe to use from sync
/// code, async code and `poll` alike.
///
/// Removed values are returned, so they drop after the lock is released.
pub(crate) struct PeerMap<V> {
    peers: Mutex<HashMap<PeerIdentity, V>>,
}

impl<V> Default for PeerMap<V> {
    fn default() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }
}

impl<V> PeerMap<V> {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.peers.lock().len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.peers.lock().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, peer_id: &PeerIdentity) -> bool {
        self.peers.lock().contains_key(peer_id)
    }

    pub(crate) fn insert(&self, peer_id: PeerIdentity, value: V) -> Option<V> {
        self.peers.lock().insert(peer_id, value)
    }

    pub(crate) fn remove(&self, peer_id: &PeerIdentity) -> Option<V> {
        self.peers.lock().remove(peer_id)
    }

    /// Removes the peer only if `condition` holds for its value.
    pub(crate) fn remove_if(
        &self,
        peer_id: &PeerIdentity,
        condition: impl FnOnce(&V) -> bool,
    ) -> Option<V> {
        let mut peers = self.peers.lock();
        if condition(peers.get(peer_id)?) {
            peers.remove(peer_id)
        } else {
            None
        }
    }

    pub(crate) fn read<R>(
        &self,
        peer_id: &PeerIdentity,
        reader: impl FnOnce(&V) -> R,
    ) -> Option<R> {
        self.peers.lock().get(peer_id).map(reader)
    }

    pub(crate) fn update<R>(
        &self,
        peer_id: &PeerIdentity,
        updater: impl FnOnce(&mut V) -> R,
    ) -> Option<R> {
        self.peers.lock().get_mut(peer_id).map(updater)
    }

    /// Calls `visit` for each peer under the lock. `visit` must not block.
    pub(crate) fn for_each(&self, mut visit: impl FnMut(&PeerIdentity, &V)) {
        for (peer_id, value) in self.peers.lock().iter() {
            visit(peer_id, value);
        }
    }

    /// Collects what `select` returns for each peer, to use after the lock
    /// is released.
    pub(crate) fn collect<R>(
        &self,
        mut select: impl FnMut(&PeerIdentity, &V) -> Option<R>,
    ) -> Vec<R> {
        self.peers
            .lock()
            .iter()
            .filter_map(|(peer_id, value)| select(peer_id, value))
            .collect()
    }

    /// Returns the peer count, and what `select` returns for the peer when
    /// there is exactly one.
    pub(crate) fn single<R>(
        &self,
        select: impl FnOnce(&PeerIdentity, &V) -> R,
    ) -> (usize, Option<R>) {
        let peers = self.peers.lock();
        let single = match peers.len() {
            1 => peers
                .iter()
                .next()
                .map(|(peer_id, value)| select(peer_id, value)),
            _ => None,
        };
        (peers.len(), single)
    }

    pub(crate) fn clear(&self) {
        let peers = std::mem::take(&mut *self.peers.lock());
        drop(peers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(name: &str) -> PeerIdentity {
        name.parse().expect("peer id")
    }

    #[test]
    fn remove_if_keeps_the_peer_when_the_condition_fails() {
        let map = PeerMap::default();
        map.insert(peer("a"), 2_u64);

        assert_eq!(
            None,
            map.remove_if(&peer("a"), |connection| *connection == 1)
        );
        assert!(map.contains(&peer("a")));

        assert_eq!(
            Some(2),
            map.remove_if(&peer("a"), |connection| *connection == 2)
        );
        assert!(!map.contains(&peer("a")));
    }

    #[test]
    fn collect_sees_every_peer() {
        let map = PeerMap::default();
        map.insert(peer("a"), 1_u64);
        map.insert(peer("b"), 2_u64);

        let mut values = map.collect(|_, value| Some(*value));
        values.sort_unstable();
        assert_eq!(vec![1, 2], values);
    }
}
