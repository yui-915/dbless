#![allow(dead_code)]

use redb::{Database as RedbDatabase, ReadableTable, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

pub trait DatabaseInterface {
    // open & close
    fn open(path: &str) -> Self;
    fn memory() -> Self;
    fn close(self);

    // main methods
    fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T>;
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()>;
    fn remove(&mut self, key: &str) -> Option<()>;
    fn clear(&mut self) -> Option<()>;
    fn keys(&self) -> Vec<String>;
    fn values<T: DeserializeOwned>(&self) -> Vec<T>;
    fn entries<T: DeserializeOwned>(&self) -> Vec<(String, T)>;
    fn len(&self) -> usize;

    // helpers
    fn is_empty(&self) -> bool; // len() == 0
    fn contains_key(&self, key: &str) -> bool; // keys().contains()

    // aliases
    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()>; // insert
    fn delete(&mut self, key: &str) -> Option<()>; // remove
    fn reset(&mut self) -> Option<()>; // clear
    fn remove_all(&mut self) -> Option<()>; // clear
    fn contains(&self, key: &str) -> bool; // contains_key
    fn has(&self, key: &str) -> bool; // contains_key
    fn size(&self) -> usize; // len
}

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("dbless");

pub struct Database {
    store: Store,
}

use Store::*;
enum Store {
    DB(RedbDatabase),
    Memory(HashMap<String, Vec<u8>>),
}

impl DatabaseInterface for Database {
    fn open(path: &str) -> Self {
        Database {
            store: DB(RedbDatabase::create(path).unwrap()),
        }
    }

    fn memory() -> Self {
        Database {
            store: Memory(HashMap::new()),
        }
    }

    fn close(self) {
        drop(self);
    }

    fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        match &self.store {
            DB(db) => {
                let bytes = db
                    .begin_read()
                    .ok()?
                    .open_table(TABLE)
                    .ok()?
                    .get(key)
                    .ok()??;
                rmp_serde::from_slice(bytes.value()).ok()
            }
            Memory(mem) => {
                let bytes = mem.get(key)?;
                rmp_serde::from_slice(bytes).ok()
            }
        }
    }

    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()> {
        match &mut self.store {
            DB(db) => {
                let bytes = rmp_serde::to_vec(value).unwrap();
                let tnx = db.begin_write().ok()?;
                tnx.open_table(TABLE)
                    .ok()?
                    .insert(key, bytes.as_slice())
                    .ok()?;
                tnx.commit().ok()?;
                Some(())
            }
            Memory(mem) => {
                let bytes = rmp_serde::to_vec(value).unwrap();
                mem.insert(key.to_string(), bytes);
                Some(())
            }
        }
    }

    fn remove(&mut self, key: &str) -> Option<()> {
        match &mut self.store {
            DB(db) => {
                let tnx = db.begin_write().ok()?;
                tnx.open_table(TABLE).ok()?.remove(key).ok()?;
                tnx.commit().ok()?;
                Some(())
            }
            Memory(mem) => {
                mem.remove(key);
                Some(())
            }
        }
    }

    fn clear(&mut self) -> Option<()> {
        match &mut self.store {
            DB(db) => {
                let tnx = db.begin_write().ok()?;
                tnx.delete_table(TABLE).ok()?;
                tnx.commit().ok()?;
                Some(())
            }
            Memory(mem) => {
                mem.clear();
                Some(())
            }
        }
    }

    fn keys(&self) -> Vec<String> {
        (|| match &self.store {
            DB(db) => {
                let tnx = db.begin_read().ok()?;
                let table = tnx.open_table(TABLE).ok()?;
                let keys = table
                    .iter()
                    .ok()?
                    .filter(|e| e.is_ok())
                    .map(|e| e.unwrap())
                    .map(|(k, _)| k.value().to_string())
                    .collect();
                Some(keys)
            }
            Memory(mem) => Some(mem.keys().cloned().collect()),
        })()
        .unwrap_or_default()
    }

    fn values<T: DeserializeOwned>(&self) -> Vec<T> {
        self.keys()
            .iter()
            .map(|k| self.get(k))
            .filter(|e| e.is_some())
            .map(|e| e.unwrap())
            .collect()
    }

    fn entries<T: DeserializeOwned>(&self) -> Vec<(String, T)> {
        self.keys()
            .iter()
            .map(|k| (k, self.get(k)))
            .filter(|e| e.1.is_some())
            .map(|e| (e.0.to_owned(), e.1.unwrap()))
            .collect()
    }

    fn len(&self) -> usize {
        self.keys().len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_key(&self, key: &str) -> bool {
        self.keys().iter().any(|e| e == key)
    }

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()> {
        self.insert(key, value)
    }

    fn delete(&mut self, key: &str) -> Option<()> {
        self.remove(key)
    }

    fn reset(&mut self) -> Option<()> {
        self.clear()
    }

    fn remove_all(&mut self) -> Option<()> {
        self.clear()
    }

    fn size(&self) -> usize {
        self.len()
    }

    fn contains(&self, key: &str) -> bool {
        self.contains_key(key)
    }

    fn has(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory() {
        let db = Database::memory();
        test_database(db);
    }

    #[test]
    fn test_redb() {
        let _ = std::fs::remove_file("test.db");

        let db = Database::open("test.db");
        test_database(db);

        let db2 = Database::open("test.db");
        test_existing_database(db2);

        std::fs::remove_file("test.db").unwrap();
    }

    fn test_database(mut db: Database) {
        // empty database
        assert_eq!(db.get::<u32>("test"), None);

        // set and get
        assert_eq!(db.set("test", &69_u32), Some(()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));

        assert_eq!(db.set("test2", &"hello world"), Some(()));
        assert_eq!(db.get::<String>("test2"), Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));

        // wrong type
        assert_eq!(db.get::<i32>("test2"), None);
    }

    fn test_existing_database(db: Database) {
        assert_eq!(db.get::<String>("test2"), Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));
    }
}
