#![allow(dead_code)]

use redb::{Database as RedbDatabase, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

pub trait DatabaseInterface {
    fn open(path: &str) -> Self;
    fn memory() -> Self;
    fn close(self);
    fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T>;
    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()>;
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

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Option<()> {
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
