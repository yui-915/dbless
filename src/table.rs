use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

use crate::store::{Store, StoreInterface};

pub trait TableReadInterface {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;
    fn keys(&self) -> Result<Vec<String>>;
    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>>;
    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>>;
    fn len(&self) -> Result<usize>;
    fn contains_key(&self, key: &str) -> Result<bool>;
    fn is_empty(&self) -> Result<bool>;

    fn size(&self) -> Result<usize>;
    fn contains(&self, key: &str) -> Result<bool>;
    fn has(&self, key: &str) -> Result<bool>;
}

pub trait TableWriteInterface {
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
    fn remove(&mut self, key: &str) -> Result<()>;
    fn clear(&mut self) -> Result<()>;

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
    fn delete(&mut self, key: &str) -> Result<()>;
    fn reset(&mut self) -> Result<()>;
}

pub struct Table<'a> {
    pub(crate) store: &'a Store,
    pub(crate) name: &'a str,
}

pub struct TableMut<'a> {
    pub(crate) store: &'a mut Store,
    pub(crate) name: &'a str,
}

impl<'a> TableReadInterface for Table<'a> {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.store.get(self.name, key)
    }

    fn keys(&self) -> Result<Vec<String>> {
        self.store.keys(self.name)
    }

    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.store.values(self.name)
    }

    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>> {
        self.store.entries(self.name)
    }

    fn len(&self) -> Result<usize> {
        self.store.len(self.name)
    }

    fn is_empty(&self) -> Result<bool> {
        self.store.is_empty(self.name)
    }

    fn contains_key(&self, key: &str) -> Result<bool> {
        self.store.contains_key(self.name, key)
    }

    fn size(&self) -> Result<usize> {
        self.len()
    }

    fn contains(&self, key: &str) -> Result<bool> {
        self.contains_key(key)
    }

    fn has(&self, key: &str) -> Result<bool> {
        self.contains_key(key)
    }
}

impl<'a> TableReadInterface for TableMut<'a> {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        Into::<Table>::into(self).get(key)
    }

    fn keys(&self) -> Result<Vec<String>> {
        Into::<Table>::into(self).keys()
    }

    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        Into::<Table>::into(self).values()
    }

    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>> {
        Into::<Table>::into(self).entries()
    }

    fn len(&self) -> Result<usize> {
        Into::<Table>::into(self).len()
    }

    fn is_empty(&self) -> Result<bool> {
        Into::<Table>::into(self).is_empty()
    }

    fn contains_key(&self, key: &str) -> Result<bool> {
        Into::<Table>::into(self).contains_key(key)
    }

    fn size(&self) -> Result<usize> {
        Into::<Table>::into(self).size()
    }

    fn contains(&self, key: &str) -> Result<bool> {
        Into::<Table>::into(self).contains(key)
    }

    fn has(&self, key: &str) -> Result<bool> {
        Into::<Table>::into(self).has(key)
    }
}

impl<'a> TableWriteInterface for TableMut<'a> {
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        self.store.insert(self.name, key, value)
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.store.remove(self.name, key)
    }

    fn clear(&mut self) -> Result<()> {
        self.store.clear(self.name)
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
}

impl<'a> From<TableMut<'a>> for Table<'a> {
    fn from(table: TableMut<'a>) -> Self {
        Self {
            store: table.store,
            name: table.name,
        }
    }
}

impl<'a> From<&'a TableMut<'a>> for Table<'a> {
    fn from(table: &'a TableMut<'a>) -> Self {
        Self {
            store: table.store,
            name: table.name,
        }
    }
}
