#![deny(missing_docs)]
#![doc = include_str!("../README.md")]
//! ## About the default table
//! Using methods from [`TableReadInterface`](trait.TableReadInterface.html) and [`TableWriteInterface`](trait.TableWriteInterface.html) directly on [`Database`](struct.Database.html) \
//! uses a default table named `#_#_main_dbless_table_#_#`.
//!
//! calling [`clear()`](struct.Database.html#method.clear) or [`reset()`](struct.Database.html#method.reset) will only clear this table, not the entire database, \
//! to clear the entire database, use [`clear_all_tables()`](struct.Database.html#method.clear_all_tables) or [`reset_all_tables()`](struct.Database.html#method.reset_all_tables).
//!
//! similarly, calling [`len()`](struct.Database.html#method.len) or [`size()`](struct.Database.html#method.size) will only count the number of entries in this table, \
//! to count the number of entries in the entire database, use [`len_all_tables()`](struct.Database.html#method.len_all_tables) or [`size_all_tables()`](struct.Database.html#method.size_all_tables).

mod store;
use store::Store;

mod table;

#[cfg(test)]
mod tests;

pub use table::{Table, TableMut, TableReadInterface, TableWriteInterface};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

const MAIN_TABLE: &str = "#_#_main_dbless_table_#_#";

/// A Database
pub struct Database {
    store: Store,
}

impl Database {
    /// Opens a file at the given path and uses it as the database.
    /// If the file doesn't exist, it will be created.
    pub fn open(path: &str) -> Result<Self> {
        Ok(Database {
            store: Store::file(path)?,
        })
    }

    /// Opens an in-memory database.
    /// Useful for tests and as a stub for a database that doesn't need to be saved to disk.
    pub fn in_memory() -> Result<Self> {
        Ok(Database {
            store: Store::in_memory()?,
        })
    }

    /// Closes the databas
    pub fn close(self) {
        drop(self);
    }

    /// Get a read-only handle to a table with the given name.
    pub fn table<'a>(&'a self, name: &'a str) -> Table<'a> {
        Table {
            store: &self.store,
            name,
        }
    }

    /// Get a read-write handle to a table with the given name.
    pub fn table_mut<'a>(&'a mut self, name: &'a str) -> TableMut<'a> {
        TableMut {
            store: &mut self.store,
            name,
        }
    }

    /// Returns a list of the names of all tables in the database.
    #[doc(hidden)] // needs more testing and consideration, especially with empty tables
    pub fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self
            .store
            .list_tables()?
            .into_iter()
            .filter(|t| t != MAIN_TABLE)
            .collect())
    }

    /// Returns the number of entries in all tables in the database. \
    /// aliases: [`size_all_tables()`](#method.size_all_tables)
    pub fn len_all_tables(&self) -> Result<usize> {
        self.store.len_all_tables()
    }

    /// Returns the number of entries in all tables in the database. \
    /// aliases: [`len_all_tables()`](#method.len_all_tables)
    pub fn size_all_tables(&self) -> Result<usize> {
        self.len_all_tables()
    }

    /// Clears all tables in the database. \
    /// aliases: [`reset_all_tables()`](#method.reset_all_tables)
    pub fn clear_all_tables(&mut self) -> Result<()> {
        self.store.clear_all_tables()
    }

    /// Clears all tables in the database. \
    /// aliases: [`clear_all_tables()`](#method.clear_all_tables)
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
