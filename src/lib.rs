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
use std::path::Path;

use store::Store;

mod table;

#[cfg(test)]
mod tests;

pub use table::{Table, TableMut, TableReadInterface, TableWriteInterface};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

const DEFAULT_DEFAULT_TABLE: &str = "#_#_main_dbless_table_#_#";

/// A Database
pub struct Database {
    store: Store,
    default_table: String,
}

impl Database {
    /// Opens a file at the given path and uses it as the database. \
    /// If the file doesn't exist, it will be created.
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::open("my_database.db")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Database {
            store: Store::file(path)?,
            default_table: String::from(DEFAULT_DEFAULT_TABLE),
        })
    }

    /// Opens an in-memory database. \
    /// Useful for tests and as a stub for a database that doesn't need to be saved to disk.
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::in_memory()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn in_memory() -> Result<Self> {
        Ok(Database {
            store: Store::in_memory()?,
            default_table: String::from(DEFAULT_DEFAULT_TABLE),
        })
    }

    /// Closes the database
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::open("my_database.db")?;
    /// db.close();
    /// // why ?
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn close(self) {
        drop(self);
    }

    /// Get a read-only handle to a table with the given name.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value = db.table("my_table").get("key")?;
    /// # let tmp: Option<String> = value;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table<'a>(&'a self, name: &'a str) -> Table<'a> {
        Table {
            store: &self.store,
            name,
        }
    }

    /// Get a read-write handle to a table with the given name.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.table_mut("my_table").set("key", &"value")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table_mut<'a>(&'a mut self, name: &'a str) -> TableMut<'a> {
        TableMut {
            store: &mut self.store,
            name,
        }
    }

    /// Returns a list of the names of all tables in the database. \
    /// This list does not include the default table.
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::open("my_database.db")?;
    /// let tables = db.list_tables()?;
    /// for table in tables {
    ///     println!("{}", table);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self
            .store
            .list_tables()?
            .into_iter()
            .filter(|t| t != &self.default_table)
            .collect())
    }

    /// Deletes a table from the database.
    /// ```no_run
    /// # use dbless::Database;
    /// let mut db = Database::open("my_database.db")?;
    /// db.delete_table("my_table")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete_table(&mut self, name: &str) -> Result<()> {
        self.store.delete_table(name)
    }

    /// Returns the number of entries in all tables in the database. \
    /// aliases: [`size_all_tables()`](#method.size_all_tables)
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::open("my_database.db")?;
    /// let len = db.len_all_tables()?;
    /// println!("the database has {} entries across all tables", len);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn len_all_tables(&self) -> Result<usize> {
        self.store.len_all_tables()
    }

    /// Returns the number of entries in all tables in the database. \
    /// aliases: [`len_all_tables()`](#method.len_all_tables)
    /// ```no_run
    /// # use dbless::Database;
    /// let db = Database::open("my_database.db")?;
    /// let size = db.size_all_tables()?;
    /// println!("the database has {} entries across all tables", size);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn size_all_tables(&self) -> Result<usize> {
        self.len_all_tables()
    }

    /// Deletes all tables in the database. \
    /// ```no_run
    /// # use dbless::Database;
    /// let mut db = Database::open("my_database.db")?;
    /// db.delete_all_tables()?;
    /// assert!(db.list_tables()?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete_all_tables(&mut self) -> Result<()> {
        self.store.delete_all_tables()
    }

    /// Get a read-only handle to the default table.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value = db.default_table().get("key")?;
    /// let also_value = db.get("key")?;
    /// assert_eq!(value, also_value);
    /// # let tmp: Option<String> = value;
    /// # let tmp: Option<String> = also_value;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn default_table(&self) -> Table {
        Table {
            store: &self.store,
            name: &self.default_table,
        }
    }

    /// Get a read-write handle to the default table.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.default_table_mut().set("key", &"value")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn default_table_mut(&mut self) -> TableMut {
        TableMut {
            store: &mut self.store,
            name: &self.default_table,
        }
    }

    /// Set the default table name.
    /// ```no_run
    /// # use dbless::Database;
    /// let mut db = Database::open("my_database.db")?;
    /// db.set_default_table("my_table");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn set_default_table(&mut self, name: &str) {
        self.default_table = String::from(name);
    }
}

macro_rules! mirror_methods_with {
    {with .$fn:ident(...); $(fn $name:ident$(<$($gname:ident: $gty1:ident $(+$gtyr:ident)*),+>)?(&self $(,$pname:ident: $pty:ty)*) -> $ret:ty;)*} => {
        $(
            fn $name$(<$($gname: $gty1$(+$gtyr)*),+>)?(&self, $($pname: $pty),*) -> $ret {
                let table = &self.default_table;
                self.$fn(table).$name($($pname),*)
            }
        )*
    }
}

macro_rules! mirror_methods_mut_with {
    {with .$fn:ident(...); $(fn $name:ident$(<$($gname:ident: $gty1:ident $(+$gtyr:ident)*),+>)?(&mut self $(,$pname:ident: $pty:ty)*) -> $ret:ty;)*} => {
        $(
            fn $name$(<$($gname: $gty1$(+$gtyr)*),+>)?(&mut self, $($pname: $pty),*) -> $ret {
                let table = &self.default_table.clone();
                self.$fn(table).$name($($pname),*)
            }
        )*
    }
}

impl TableReadInterface for Database {
    mirror_methods_with! {
        with .table(...);
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
        let table = &self.default_table;
        self.table(table).get_or_else(key, default)
    }
}

impl TableWriteInterface for Database {
    mirror_methods_mut_with! {
        with .table_mut(...);
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
        let table = &self.default_table.clone();
        self.table_mut(table).get_or_insert_with(key, default)
    }
}
