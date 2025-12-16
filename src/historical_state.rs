use std::{borrow::Cow, collections::BTreeMap};

use anyhow::{Result, bail};

pub struct HistoricalState<T> {
    slots: BTreeMap<u64, T>,
}

impl<T: Default + Clone> HistoricalState<T> {
    pub fn new() -> Self {
        Self {
            slots: BTreeMap::new(),
        }
    }

    pub fn latest(&self) -> Cow<'_, T> {
        match self.slots.last_key_value() {
            Some((_, v)) => Cow::Borrowed(v),
            None => Cow::Owned(T::default()),
        }
    }

    pub fn update_slot(&mut self, slot: u64) -> Result<&mut T> {
        let Some((&latest_slot, _)) = self.slots.last_key_value() else {
            return Ok(self.slots.entry(slot).or_default());
        };
        if latest_slot > slot {
            bail!("cannot update slot {slot} because we are on slot {latest_slot}");
        }
        if latest_slot == slot {
            return Ok(self.slots.get_mut(&slot).unwrap());
        }
        let last_entry = match self.slots.range(..slot).last() {
            Some((_, e)) => e.clone(),
            None => T::default(),
        };
        Ok(self.slots.entry(slot).or_insert(last_entry))
    }

    pub fn prune_history(&mut self, rollback_limit: u64) -> bool {
        let mut pruned = false;
        while self.slots.len() > rollback_limit as usize {
            self.slots.pop_first();
            pruned = true;
        }
        pruned
    }

    pub fn rollback_to_slot(&mut self, slot: u64) -> Vec<(u64, T)> {
        let mut rolled_back = vec![];
        while self.slots.last_key_value().is_some_and(|(s, _)| *s > slot) {
            rolled_back.push(self.slots.pop_last().unwrap());
        }
        rolled_back
    }

    pub fn rollback_to_origin(&mut self) {
        self.slots.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_copy_old_state_into_new_slot() -> Result<()> {
        let mut history = HistoricalState::<Vec<u8>>::new();
        assert!(history.latest().is_empty());

        history.update_slot(0)?.push(1);
        assert_eq!(history.latest().as_ref(), &[1]);

        history.update_slot(1)?.push(2);
        assert_eq!(history.latest().as_ref(), &[1, 2]);

        Ok(())
    }

    #[test]
    fn should_preserve_old_state_on_rollback() -> Result<()> {
        let mut history = HistoricalState::<Vec<u8>>::new();
        assert!(history.latest().is_empty());

        history.update_slot(0)?.push(1);
        assert_eq!(history.latest().as_ref(), &[1]);

        history.update_slot(1)?.push(2);
        assert_eq!(history.latest().as_ref(), &[1, 2]);

        history.rollback_to_slot(0);
        assert_eq!(history.latest().as_ref(), &[1]);

        Ok(())
    }

    #[test]
    fn should_not_allow_out_of_order_updates() -> Result<()> {
        let mut history = HistoricalState::<Vec<u8>>::new();
        assert!(history.latest().is_empty());

        history.update_slot(0)?.push(1);
        assert_eq!(history.latest().as_ref(), &[1]);

        history.update_slot(1)?.push(2);
        assert_eq!(history.latest().as_ref(), &[1, 2]);

        assert!(history.update_slot(0).is_err());
        Ok(())
    }
}
