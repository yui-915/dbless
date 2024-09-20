use anyhow::Result;
use redb::{backends::InMemoryBackend, Builder, Database, TableError, TableHandle};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

pub struct Store(Database);

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = rmp_serde::Serializer::new(vec![]).with_struct_map();
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

fn deserialize<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(value)?)
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

impl Store {
    pub fn file(path: &str) -> Result<Self> {
        let db = Database::create(path)?;
        Ok(Store(db))
    }

    pub fn in_memory() -> Result<Self> {
        let backend = InMemoryBackend::new();
        let db = Builder::new().create_with_backend(backend)?;
        Ok(Store(db))
    }

    pub fn get<T: DeserializeOwned>(&self, table: &str, key: &str) -> Result<Option<T>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, None);
        let bytes = match table.get(key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        deserialize(bytes.value())
    }

    pub fn insert<T: Serialize>(&mut self, table: &str, key: &str, value: &T) -> Result<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let bytes = serialize(value)?;
        let db = &self.0;
        let tnx = db.begin_write()?;
        {
            let mut table = tnx.open_table(table)?;
            table.insert(key, bytes.as_slice())?;
        }
        tnx.commit()?;
        Ok(())
    }

    pub fn remove(&mut self, table: &str, key: &str) -> Result<()> {
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

    pub fn clear(&mut self, table: &str) -> Result<()> {
        let table = TableDefinition::<&str, &[u8]>::new(table);
        let db = &self.0;
        let tnx = db.begin_write()?;
        tnx.delete_table(table)?;
        tnx.commit()?;
        Ok(())
    }

    pub fn keys(&self, table: &str) -> Result<Vec<String>> {
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

    pub fn values<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<T>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, vec![]);
        let entries = table.iter()?;
        let values = entries
            .flatten()
            .flat_map(|(_, v)| deserialize(v.value()).ok())
            .collect();
        Ok(values)
    }

    pub fn entries<T: DeserializeOwned>(&self, table: &str) -> Result<Vec<(String, T)>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, vec![]);
        let entries = table.iter()?;
        let entries = entries
            .flatten()
            .flat_map(|(k, v)| Some((k.value().to_string(), deserialize(v.value()).ok()?)))
            .collect();
        Ok(entries)
    }

    pub fn len(&self, table: &str) -> Result<usize> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, 0);
        let len = table.len()?;
        Ok(len as usize)
    }

    pub fn contains_key(&self, table: &str, key: &str) -> Result<bool> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let table = open_table_read_or!(tnx, table, false);
        Ok(table.get(key)?.is_some())
    }

    pub fn is_empty(&self, table: &str) -> Result<bool> {
        Ok(self.len(table)? == 0)
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let tables = tnx.list_tables()?;
        Ok(tables.map(|t| t.name().to_string()).collect())
    }

    pub fn len_all_tables(&self) -> Result<usize> {
        let db = &self.0;
        let tnx = db.begin_read()?;
        let tables = tnx.list_tables()?;
        let mut len = 0;
        for t in tables {
            let table_definition = TableDefinition::<&str, &[u8]>::new(t.name());
            let table = tnx.open_table(table_definition)?;
            len += table.len()?;
        }
        Ok(len as usize)
    }

    pub fn clear_all_tables(&mut self) -> Result<()> {
        let db = &self.0;
        let tnx = db.begin_write()?;
        let tables = tnx.list_tables()?;
        for table in tables {
            tnx.delete_table(table)?;
        }
        tnx.commit()?;
        Ok(())
    }
}
