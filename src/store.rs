pub mod memory;
pub mod redb;

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

pub enum Store {
    Redb(redb::Store),
    Memory(memory::Store),
}

pub trait StoreInterface {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>>;
    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Result<()>;
    fn remove(&mut self, table: &str, key: &str) -> Result<()>;
    fn clear(&mut self, table: &str) -> Result<()>;
    fn keys(&self, table: &str) -> Result<Vec<String>>;
    fn values<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<T>>;
    fn entries<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<(String, T)>>;
    fn len(&self, table: &str) -> Result<usize>;
    fn contains_key(&self, table: &str, key: &str) -> Result<bool>;
    fn is_empty(&self, table: &str) -> Result<bool>;
    fn list_tables(&self) -> Result<Vec<String>>;
    fn len_all_tables(&self) -> Result<usize>;
    fn clear_all_tables(&mut self) -> Result<()>;
}

impl StoreInterface for Store {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>> {
        match self {
            Store::Redb(store) => store.get(table, key),
            Store::Memory(store) => store.get(table, key),
        }
    }

    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Result<()> {
        match self {
            Store::Redb(store) => store.insert(table, key, value),
            Store::Memory(store) => store.insert(table, key, value),
        }
    }

    fn remove(&mut self, table: &str, key: &str) -> Result<()> {
        match self {
            Store::Redb(store) => store.remove(table, key),
            Store::Memory(store) => store.remove(table, key),
        }
    }

    fn clear(&mut self, table: &str) -> Result<()> {
        match self {
            Store::Redb(store) => store.clear(table),
            Store::Memory(store) => store.clear(table),
        }
    }

    fn keys(&self, table: &str) -> Result<Vec<String>> {
        match self {
            Store::Redb(store) => store.keys(table),
            Store::Memory(store) => store.keys(table),
        }
    }

    fn values<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<T>> {
        match self {
            Store::Redb(store) => store.values(table),
            Store::Memory(store) => store.values(table),
        }
    }

    fn entries<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<(String, T)>> {
        match self {
            Store::Redb(store) => store.entries(table),
            Store::Memory(store) => store.entries(table),
        }
    }

    fn len(&self, table: &str) -> Result<usize> {
        match self {
            Store::Redb(store) => store.len(table),
            Store::Memory(store) => store.len(table),
        }
    }

    fn contains_key(&self, table: &str, key: &str) -> Result<bool> {
        match self {
            Store::Redb(store) => store.contains_key(table, key),
            Store::Memory(store) => store.contains_key(table, key),
        }
    }

    fn is_empty(&self, table: &str) -> Result<bool> {
        match self {
            Store::Redb(store) => store.is_empty(table),
            Store::Memory(store) => store.is_empty(table),
        }
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        match self {
            Store::Redb(store) => store.list_tables(),
            Store::Memory(store) => store.list_tables(),
        }
    }

    fn len_all_tables(&self) -> Result<usize> {
        match self {
            Store::Redb(store) => store.len_all_tables(),
            Store::Memory(store) => store.len_all_tables(),
        }
    }

    fn clear_all_tables(&mut self) -> Result<()> {
        match self {
            Store::Redb(store) => store.clear_all_tables(),
            Store::Memory(store) => store.clear_all_tables(),
        }
    }
}
