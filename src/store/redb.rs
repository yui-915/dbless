use crate::StoreInterface;
use anyhow::Result;
use redb::{Database, TableError};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

pub struct Store(Database);

impl Store {
    pub(crate) fn new(path: &str) -> Result<Self> {
        Ok(Store(Database::create(path)?))
    }
}

macro_rules! open_table_read_or {
    ($tnx:expr, $table:expr, $or:expr) => {
        match $tnx.open_table(TableDefinition::<&str, &[u8]>::new($table)) {
            Ok(table) => table,
            Err(e) => match e {
                TableError::TableDoesNotExist(_) => return Ok($or),
                _ => return Err(e.into()),
            },
        }
    };
}

impl StoreInterface for Store {
    fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, None);
        let bytes = match table.get(key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        Ok(rmp_serde::from_slice(bytes.value())?)
    }

    fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Result<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let bytes = rmp_serde::to_vec(value)?;
        let db = &self.0;
        let tnx = db.begin_write()?;
        {
            let mut table = tnx.open_table(table)?;
            table.insert(key, bytes.as_slice())?;
        }
        tnx.commit()?;
        Ok(())
    }

    fn remove(&mut self, table: &str, key: &str) -> Result<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_write()?;
        {
            let mut table = tnx.open_table(table)?;
            table.remove(key)?;
        }
        tnx.commit()?;
        Ok(())
    }

    fn clear(&mut self, table: &str) -> Result<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_write()?;
        tnx.delete_table(table)?;
        tnx.commit()?;
        Ok(())
    }

    fn keys(&self, table: &str) -> Result<Vec<String>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, vec![]);
        let entries = table.iter()?;
        let keys = entries
            .flatten()
            .map(|(k, _)| k.value().to_string())
            .collect();
        Ok(keys)
    }

    fn values<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<T>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, vec![]);
        let entries = table.iter()?;
        let values = entries
            .flatten()
            .flat_map(|(_, v)| rmp_serde::from_slice(v.value()).ok())
            .collect();
        Ok(values)
    }

    fn entries<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<(String, T)>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, vec![]);
        let entries = table.iter()?;
        let entries = entries
            .flatten()
            .flat_map(|(k, v)| {
                Some((
                    k.value().to_string(),
                    rmp_serde::from_slice(v.value()).ok()?,
                ))
            })
            .collect();
        Ok(entries)
    }

    fn len(&self, table: &str) -> Result<usize> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, 0);
        let len = table.len()?;
        Ok(len as usize)
    }

    fn contains_key(&self, table: &str, key: &str) -> Result<bool> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, false);
        Ok(table.get(key)?.is_some())
    }

    fn is_empty(&self, table: &str) -> Result<bool> {
        Ok(self.len(table)? == 0)
    }
}
