#![allow(dead_code)]

mod store;
use store::{Store, StoreInterface};

mod table;
pub use table::{Table, TableMut, TableReadInterface, TableWriteInterface};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

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

    fn remove_all(&mut self) -> Result<()> {
        self.clear()
    }
}

// TODO: rewrite and move to a separate file
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory() -> Result<()> {
        let db = Database::memory();
        test_database(db)?;
        Ok(())
    }

    #[test]
    fn test_redb() -> Result<()> {
        let _ = std::fs::remove_file("test.db");

        let db = Database::open("test.db");
        test_database(db)?;

        let db2 = Database::open("test.db");
        test_existing_database(db2)?;

        std::fs::remove_file("test.db").unwrap();
        Ok(())
    }

    fn test_database(mut db: Database) -> Result<()> {
        // empty database
        assert_eq!(db.get::<u32>("test")?, None);
        assert!(db.is_empty()?);
        assert_eq!(db.len()?, 0);
        assert_eq!(db.keys()?.len(), 0);
        assert_eq!(db.values::<u32>()?, vec![]);
        assert_eq!(db.entries::<String>()?, vec![]);
        assert!(!db.contains_key("test")?);

        // set and get
        db.set("test", &69_u32)?;
        assert_eq!(db.get::<u32>("test")?, Some(69_u32));
        assert!(!db.is_empty()?);
        assert_eq!(db.len()?, 1);
        assert_eq!(db.size()?, 1);
        assert_eq!(db.keys()?, vec!["test"]);
        assert_eq!(db.values::<u32>()?, vec![69_u32]);
        assert_eq!(db.entries::<u32>()?, vec![("test".to_string(), 69_u32)]);
        assert!(db.contains_key("test")?);
        assert!(db.contains("test")?);
        assert!(!db.contains("test2")?);
        assert!(db.has("test")?);

        db.set("test2", &"hello world")?;
        assert_eq!(db.get::<String>("test2")?, Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test")?, Some(69_u32));

        // wrong type
        assert!(db.get::<i32>("test2").is_err());

        // keys, values, entries
        let mut keys = db.keys()?;
        let mut expected = ["test", "test2"];
        keys.sort();
        expected.sort();
        assert_eq!(keys, expected);
        assert_eq!(db.values::<u32>()?, vec![69_u32]);
        assert_eq!(
            db.entries::<String>()?,
            vec![("test2".to_string(), "hello world".to_string())]
        );

        // remove
        db.remove("test3")?;
        db.remove("test2")?;
        assert_eq!(db.get::<String>("test2")?, None);

        // empty again
        db.clear()?;
        db.clear()?;
        assert!(db.is_empty()?);
        assert_eq!(db.len()?, 0);
        assert_eq!(db.keys()?.len(), 0);
        assert_eq!(db.values::<u32>()?, vec![]);
        assert_eq!(db.entries::<String>()?, vec![]);
        assert!(!db.contains_key("test")?);

        // for later
        db.set("test3", &"hello world")?;
        db.set("test", &69_u32)?;

        Ok(())
    }

    fn test_existing_database(db: Database) -> Result<()> {
        assert_eq!(db.len()?, 2);
        assert_eq!(db.keys()?, vec!["test", "test3"]);
        assert_eq!(db.values::<u32>()?, vec![69_u32]);
        assert_eq!(db.entries::<u32>()?, vec![("test".to_string(), 69_u32)]);
        assert_eq!(db.get::<String>("test3")?, Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test")?, Some(69_u32));
        assert!(!db.is_empty()?);

        Ok(())
    }
}
