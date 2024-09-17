use crate::StoreInterface;
use serde::{de::DeserializeOwned, Serialize};
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
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Option<T> {
        let table = self.table(table)?;
        let bytes = table.get(key)?;
        rmp_serde::from_slice(bytes).ok()
    }

    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Option<()> {
        let table = self.table_mut(table);
        table.insert(key.to_string(), rmp_serde::to_vec(value).unwrap());
        Some(())
    }

    fn remove(&mut self, table: &str, key: &str) -> Option<()> {
        let table = self.table_mut(table);
        table.remove(key).map(|_| ())
    }

    fn clear(&mut self, table: &str) -> Option<()> {
        let table = self.table_mut(table);
        let res = if table.is_empty() { None } else { Some(()) };
        table.clear();
        res
    }

    fn keys(&self, table: &str) -> Vec<String> {
        self.table(table)
            .map(|table| table.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn values<T: DeserializeOwned>(&self, table: &str) -> Vec<T> {
        self.table(table)
            .map(|table| {
                table
                    .values()
                    .filter_map(|bytes| rmp_serde::from_slice(bytes).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn entries<T: DeserializeOwned>(&self, table: &str) -> Vec<(String, T)> {
        self.table(table)
            .map(|table| {
                table
                    .iter()
                    .filter_map(|(key, bytes)| {
                        let value = rmp_serde::from_slice(bytes).ok()?;
                        Some((key.clone(), value))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn len(&self, table: &str) -> usize {
        self.table(table)
            .map(|table| table.len())
            .unwrap_or_default()
    }

    fn contains_key(&self, table: &str, key: &str) -> bool {
        self.table(table)
            .map(|table| table.contains_key(key))
            .unwrap_or_default()
    }

    fn is_empty(&self, table: &str) -> bool {
        self.table(table)
            .map(|table| table.is_empty())
            .unwrap_or(true)
    }
}
