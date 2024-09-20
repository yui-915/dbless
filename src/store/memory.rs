use crate::serde::{deserialize, serialize, DeserializeOwned, Serialize};
use crate::StoreInterface;
use anyhow::Result;
use std::collections::HashMap;

type Table = HashMap<String, Vec<u8>>;
pub struct Store(HashMap<String, Table>);

impl Store {
    pub(crate) fn new() -> Self {
        Store(HashMap::new())
    }
}

impl Store {
    fn table(&self, table: &str) -> Option<&Table> {
        self.0.get(table)
    }

    fn table_mut(&mut self, table: &str) -> &mut Table {
        self.0.entry(table.to_string()).or_default()
    }
}

impl StoreInterface for Store {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(None),
        };
        let bytes = match table.get(key) {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        deserialize(bytes).map(Some)
    }

    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Result<()> {
        let table = self.table_mut(table);
        table.insert(key.to_string(), serialize(value)?);
        Ok(())
    }

    fn remove(&mut self, table: &str, key: &str) -> Result<()> {
        let table = self.table_mut(table);
        table.remove(key);
        Ok(())
    }

    fn clear(&mut self, table: &str) -> Result<()> {
        let table = self.table_mut(table);
        table.clear();
        Ok(())
    }

    fn keys(&self, table: &str) -> Result<Vec<String>> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(Vec::new()),
        };
        let keys = table.keys().cloned().collect();
        Ok(keys)
    }

    fn values<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<T>> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(Vec::new()),
        };
        let values = table
            .values()
            .filter_map(|bytes| deserialize(bytes).ok())
            .collect();
        Ok(values)
    }

    fn entries<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<(String, T)>> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(Vec::new()),
        };
        let entries = table
            .iter()
            .filter_map(|(key, bytes)| {
                let value = deserialize(bytes).ok()?;
                Some((key.clone(), value))
            })
            .collect();
        Ok(entries)
    }

    fn len(&self, table: &str) -> Result<usize> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(0),
        };
        Ok(table.len())
    }

    fn contains_key(&self, table: &str, key: &str) -> Result<bool> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(false),
        };
        Ok(table.contains_key(key))
    }

    fn is_empty(&self, table: &str) -> Result<bool> {
        let table = match self.table(table) {
            Some(table) => table,
            None => return Ok(true),
        };
        Ok(table.is_empty())
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self.0.keys().cloned().collect())
    }

    fn len_all_tables(&self) -> Result<usize> {
        Ok(self.0.values().map(|v| v.len()).sum())
    }

    fn clear_all_tables(&mut self) -> Result<()> {
        self.0.clear();
        Ok(())
    }
}
