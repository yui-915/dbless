#![allow(dead_code)]

mod memory_store;
mod redb_store;

use serde::{de::DeserializeOwned, Serialize};

enum Store {
    Redb(redb_store::Store),
    Memory(memory_store::Store),
}

trait StoreInterface {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Option<T>;
    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Option<()>;
    fn remove(&mut self, table: &str, key: &str) -> Option<()>;
    fn clear(&mut self, table: &str) -> Option<()>;
    fn keys(&self, table: &str) -> Vec<String>;
    fn values<T: DeserializeOwned>(&self, table: &str) -> Vec<T>;
    fn entries<T: DeserializeOwned>(&self, table: &str) -> Vec<(String, T)>;
    fn len(&self, table: &str) -> usize;
    fn contains_key(&self, table: &str, key: &str) -> bool;
    fn is_empty(&self, table: &str) -> bool;
}

impl StoreInterface for Store {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Option<T> {
        match self {
            Store::Redb(store) => store.get(table, key),
            Store::Memory(store) => store.get(table, key),
        }
    }

    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Option<()> {
        match self {
            Store::Redb(store) => store.insert(table, key, value),
            Store::Memory(store) => store.insert(table, key, value),
        }
    }

    fn remove(&mut self, table: &str, key: &str) -> Option<()> {
        match self {
            Store::Redb(store) => store.remove(table, key),
            Store::Memory(store) => store.remove(table, key),
        }
    }

    fn clear(&mut self, table: &str) -> Option<()> {
        match self {
            Store::Redb(store) => store.clear(table),
            Store::Memory(store) => store.clear(table),
        }
    }

    fn keys(&self, table: &str) -> Vec<String> {
        match self {
            Store::Redb(store) => store.keys(table),
            Store::Memory(store) => store.keys(table),
        }
    }

    fn values<T: DeserializeOwned>(&self, table: &str) -> Vec<T> {
        match self {
            Store::Redb(store) => store.values(table),
            Store::Memory(store) => store.values(table),
        }
    }

    fn entries<T: DeserializeOwned>(&self, table: &str) -> Vec<(String, T)> {
        match self {
            Store::Redb(store) => store.entries(table),
            Store::Memory(store) => store.entries(table),
        }
    }

    fn len(&self, table: &str) -> usize {
        match self {
            Store::Redb(store) => store.len(table),
            Store::Memory(store) => store.len(table),
        }
    }

    fn contains_key(&self, table: &str, key: &str) -> bool {
        match self {
            Store::Redb(store) => store.contains_key(table, key),
            Store::Memory(store) => store.contains_key(table, key),
        }
    }

    fn is_empty(&self, table: &str) -> bool {
        match self {
            Store::Redb(store) => store.is_empty(table),
            Store::Memory(store) => store.is_empty(table),
        }
    }
}

pub struct Database {
    store: Store,
}

impl Database {
    fn open(path: &str) -> Self {
        Database {
            store: Store::Redb(redb_store::Store::new(path)),
        }
    }
}

impl Database {
    fn memory() -> Self {
        Database {
            store: Store::Memory(memory_store::Store::new()),
        }
    }
}

const MAIN_TABLE: &str = "dbless";

impl Database {
    fn close(self) {
        drop(self);
    }

    fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.store.get(MAIN_TABLE, key)
    }

    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()> {
        self.store.insert(MAIN_TABLE, key, value)
    }

    fn remove(&mut self, key: &str) -> Option<()> {
        self.store.remove(MAIN_TABLE, key)
    }

    fn clear(&mut self) -> Option<()> {
        self.store.clear(MAIN_TABLE)
    }

    fn keys(&self) -> Vec<String> {
        self.store.keys(MAIN_TABLE)
    }

    fn values<T: DeserializeOwned>(&self) -> Vec<T> {
        self.store.values(MAIN_TABLE)
    }

    fn entries<T: DeserializeOwned>(&self) -> Vec<(String, T)> {
        self.store.entries(MAIN_TABLE)
    }

    fn len(&self) -> usize {
        self.store.len(MAIN_TABLE)
    }

    fn is_empty(&self) -> bool {
        self.store.is_empty(MAIN_TABLE)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.store.contains_key(MAIN_TABLE, key)
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
        assert!(db.is_empty());
        assert_eq!(db.len(), 0);
        assert_eq!(db.keys().len(), 0);
        assert_eq!(db.values::<u32>(), vec![]);
        assert_eq!(db.entries::<String>(), vec![]);
        assert!(!db.contains_key("test"));

        // set and get
        assert_eq!(db.set("test", &69_u32), Some(()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));
        assert!(!db.is_empty());
        assert_eq!(db.len(), 1);
        assert_eq!(db.size(), 1);
        assert_eq!(db.keys(), vec!["test"]);
        assert_eq!(db.values::<u32>(), vec![69_u32]);
        assert_eq!(db.entries::<u32>(), vec![("test".to_string(), 69_u32)]);
        assert!(db.contains_key("test"));
        assert!(db.contains("test"));
        assert!(!db.contains("test2"));
        assert!(db.has("test"));

        assert_eq!(db.set("test2", &"hello world"), Some(()));
        assert_eq!(db.get::<String>("test2"), Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));

        // wrong type
        assert_eq!(db.get::<i32>("test2"), None);

        // keys, values, entries
        assert_eq!(db.keys().sort(), ["test", "test2"].sort());
        assert_eq!(db.values::<u32>(), vec![69_u32]);
        assert_eq!(
            db.entries::<String>(),
            vec![("test2".to_string(), "hello world".to_string())]
        );

        // remove
        assert_eq!(db.remove("test3"), None);
        assert_eq!(db.remove("test2"), Some(()));
        assert_eq!(db.get::<String>("test2"), None);

        // empty again
        assert_eq!(db.clear(), Some(()));
        assert_eq!(db.clear(), None);
        assert!(db.is_empty());
        assert_eq!(db.len(), 0);
        assert_eq!(db.keys().len(), 0);
        assert_eq!(db.values::<u32>(), vec![]);
        assert_eq!(db.entries::<String>(), vec![]);
        assert!(!db.contains_key("test"));

        // for later
        assert_eq!(db.set("test3", &"hello world"), Some(()));
        assert_eq!(db.set("test", &69_u32), Some(()));
    }

    fn test_existing_database(db: Database) {
        assert_eq!(db.len(), 2);
        assert_eq!(db.keys(), vec!["test", "test3"]);
        assert_eq!(db.values::<u32>(), vec![69_u32]);
        assert_eq!(db.entries::<u32>(), vec![("test".to_string(), 69_u32)]);
        assert_eq!(db.get::<String>("test3"), Some("hello world".to_string()));
        assert_eq!(db.get::<u32>("test"), Some(69_u32));
        assert!(!db.is_empty());
    }
}
