#![deny(missing_docs)]
#![doc = include_str!("../README.md")]
//! ## About the default table
//! Using methods from [`TableReadInterface`](trait.TableReadInterface.html) and [`TableWriteInterface`](trait.TableWriteInterface.html) directly on [`Database`](struct.Database.html) \
//! uses a default table named `#_#_main_dbless_table_#_#`.
//!
//! calling [`clear()`](struct.Database.html#method.clear) or [`reset()`](struct.Database.html#method.reset) will only clear this table, not the entire database, \
//! to clear the entire database, use [`delete_all_tables()`](struct.Database.html#method.delete_all_tables)
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
    pub fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self
            .store
            .list_tables()?
            .into_iter()
            .filter(|t| t != MAIN_TABLE)
            .collect())
    }

    /// Deletes a table from the database.
    pub fn delete_table(&mut self, name: &str) -> Result<()> {
        self.store.delete_table(name)
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

    /// Deletes all tables in the database. \
    /// aliases: [`reset_all_tables()`](#method.reset_all_tables)
    pub fn delete_all_tables(&mut self) -> Result<()> {
        self.store.delete_all_tables()
    }
}

macro_rules! mirror_methods_with {
    {self.$fn:ident($arg:expr); $(fn $name:ident$(<$($gname:ident: $gty1:ident $(+$gtyr:ident)*),+>)?(&self $(,$pname:ident: $pty:ty)*) -> $ret:ty;)*} => {
        $(
            fn $name$(<$($gname: $gty1$(+$gtyr)*),+>)?(&self, $($pname: $pty),*) -> $ret {
                self.$fn($arg).$name($($pname),*)
            }
        )*
    }
}

macro_rules! mirror_methods_mut_with {
    {self.$fn:ident($arg:expr); $(fn $name:ident$(<$($gname:ident: $gty1:ident $(+$gtyr:ident)*),+>)?(&mut self $(,$pname:ident: $pty:ty)*) -> $ret:ty;)*} => {
        $(
            fn $name$(<$($gname: $gty1$(+$gtyr)*),+>)?(&mut self, $($pname: $pty),*) -> $ret {
                self.$fn($arg).$name($($pname),*)
            }
        )*
    }
}

impl TableReadInterface for Database {
    mirror_methods_with! {
        self.table(MAIN_TABLE);
        fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;
        fn keys(&self) -> Result<Vec<String>> ;
        fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>> ;
        fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>> ;
        fn len(&self) -> Result<usize> ;
        fn is_empty(&self) -> Result<bool> ;
        fn contains_key(&self, key: &str) -> Result<bool> ;
        fn size(&self) -> Result<usize> ;
        fn contains(&self, key: &str) -> Result<bool> ;
        fn has(&self, key: &str) -> Result<bool> ;
        fn get_or<T: DeserializeOwned>(&self, key: &str, default: T) -> Result<T> ;
        fn get_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> Result<T> ;
    }

    // current macro can't handle FnOnce() -> T
    fn get_or_else<T: DeserializeOwned, F: FnOnce() -> T>(
        &self,
        key: &str,
        default: F,
    ) -> Result<T> {
        self.table(MAIN_TABLE).get_or_else(key, default)
    }
}

impl TableWriteInterface for Database {
    mirror_methods_mut_with! {
        self.table_mut(MAIN_TABLE);
        fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
        fn remove(&mut self, key: &str) -> Result<()>;
        fn clear(&mut self) -> Result<()>;
        fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
        fn delete(&mut self, key: &str) -> Result<()>;
        fn reset(&mut self) -> Result<()>;
        fn get_or_insert<T: Serialize + DeserializeOwned>(&mut self, key: &str, default: T) -> Result<T>;
        fn get_or_insert_default<T: Serialize + DeserializeOwned + Default>(&mut self, key: &str) -> Result<T>;
    }

    // current macro can't handle FnOnce() -> T
    fn get_or_insert_with<T: Serialize + DeserializeOwned, F: FnOnce() -> T>(
        &mut self,
        key: &str,
        default: F,
    ) -> Result<T> {
        self.table_mut(MAIN_TABLE).get_or_insert_with(key, default)
    }
}
