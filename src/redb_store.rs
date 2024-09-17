use crate::StoreInterface;
use redb::Database;
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

pub struct Store(Database);

impl Store {
    pub(crate) fn new(path: &str) -> Self {
        Store(Database::create(path).unwrap())
    }
}

impl StoreInterface for Store {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Option<T> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_read().ok()?;
        let table = tnx.open_table(table).ok()?;
        let bytes = table.get(key).ok()??;
        rmp_serde::from_slice(bytes.value()).ok()
    }
    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Option<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let bytes = rmp_serde::to_vec(value).unwrap();
        let db = &self.0;
        let tnx = db.begin_write().ok()?;
        {
            let mut table = tnx.open_table(table).ok()?;
            table.insert(key, bytes.as_slice()).ok()?;
        }
        tnx.commit().ok()?;
        Some(())
    }
    fn remove(&mut self, table: &str, key: &str) -> Option<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_write().ok()?;
        let mut res = Some(());
        {
            let mut table = tnx.open_table(table).ok()?;
            if table.get(key).map_or(true, |key| key.is_none()) {
                res = None;
            } else {
                table.remove(key).ok()?;
            }
        }
        tnx.commit().ok()?;
        res
    }
    fn clear(&mut self, table: &str) -> Option<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_write().ok()?;
        {
            if tnx.open_table(table).ok()?.len().ok()? == 0 {
                return None;
            };
        }
        tnx.delete_table(table).ok()?;
        tnx.commit().ok()?;
        Some(())
    }
    fn keys(&self, table: &str) -> Vec<String> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        if let Ok(tnx) = db.begin_read() {
            if let Ok(table) = tnx.open_table(table) {
                let keys = table.iter().ok();
                keys.map(|keys| keys.flatten().map(|(k, _)| k.value().to_string()).collect())
                    .unwrap_or_default()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }
    fn values<T: DeserializeOwned>(&self, table: &str) -> Vec<T> {
        self.keys(table)
            .iter()
            .filter_map(|k| self.get(table, k))
            .collect()
    }
    fn entries<T: DeserializeOwned>(&self, table: &str) -> Vec<(String, T)> {
        self.keys(table)
            .iter()
            .map(|k| (k, self.get(table, k)))
            .filter(|e| e.1.is_some())
            .map(|e| (e.0.to_owned(), e.1.unwrap()))
            .collect()
    }
    fn len(&self, table: &str) -> usize {
        self.keys(table).len()
    }
    fn contains_key(&self, table: &str, key: &str) -> bool {
        self.keys(table).iter().any(|e| e == key)
    }
    fn is_empty(&self, table: &str) -> bool {
        self.len(table) == 0
    }
}
