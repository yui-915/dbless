mod store;
use store::{Store, StoreInterface};

mod table;

#[cfg(test)]
mod tests;

pub use table::{Table, TableMut, TableReadInterface, TableWriteInterface};

use anyhow::Result;

mod serde;
use serde::{DeserializeOwned, Serialize};

const MAIN_TABLE: &str = "#_#_main_dbless_table_#_#";

pub struct Database {
    store: Store,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        Ok(Database {
            store: Store::Redb(store::redb::Store::new(path)?),
        })
    }

    pub fn memory() -> Self {
        Database {
            store: Store::Memory(store::memory::Store::new()),
        }
    }

    pub fn close(self) {
        drop(self);
    }

    pub fn table<'a>(&'a self, name: &'a str) -> Table<'a> {
        Table {
            store: &self.store,
            name,
        }
    }

    pub fn table_mut<'a>(&'a mut self, name: &'a str) -> TableMut<'a> {
        TableMut {
            store: &mut self.store,
            name,
        }
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self
            .store
            .list_tables()?
            .into_iter()
            .filter(|t| t != MAIN_TABLE)
            .collect())
    }

    pub fn len_all_tables(&self) -> Result<usize> {
        self.store.len_all_tables()
    }

    pub fn size_all_tables(&self) -> Result<usize> {
        self.len_all_tables()
    }

    pub fn clear_all_tables(&mut self) -> Result<()> {
        self.store.clear_all_tables()
    }

    pub fn reset_all_tables(&mut self) -> Result<()> {
        self.clear_all_tables()
    }
}

impl TableReadInterface for Database {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.table(MAIN_TABLE).get(key)
    }

    fn keys(&self) -> Result<Vec<String>> {
        self.table(MAIN_TABLE).keys()
    }

    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.table(MAIN_TABLE).values()
    }

    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>> {
        self.table(MAIN_TABLE).entries()
    }

    fn len(&self) -> Result<usize> {
        self.table(MAIN_TABLE).len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.table(MAIN_TABLE).is_empty()
    }

    fn contains_key(&self, key: &str) -> Result<bool> {
        self.table(MAIN_TABLE).contains_key(key)
    }

    fn size(&self) -> Result<usize> {
        self.table(MAIN_TABLE).size()
    }

    fn contains(&self, key: &str) -> Result<bool> {
        self.table(MAIN_TABLE).contains(key)
    }

    fn has(&self, key: &str) -> Result<bool> {
        self.table(MAIN_TABLE).has(key)
    }
}

impl TableWriteInterface for Database {
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        self.table_mut(MAIN_TABLE).insert(key, value)
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.table_mut(MAIN_TABLE).remove(key)
    }

    fn clear(&mut self) -> Result<()> {
        self.table_mut(MAIN_TABLE).clear()
    }

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        self.insert(key, value)
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.remove(key)
    }

    fn reset(&mut self) -> Result<()> {
        self.clear()
    }
}
