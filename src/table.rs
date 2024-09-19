use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

pub trait TableInterface {
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
    fn remove(&mut self, key: &str) -> Result<()>;
    fn clear(&mut self) -> Result<()>;
    fn keys(&self) -> Result<Vec<String>>;
    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>>;
    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>>;
    fn len(&self) -> Result<usize>;
    fn contains_key(&self, key: &str) -> Result<bool>;
    fn is_empty(&self) -> Result<bool>;

    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
    fn delete(&mut self, key: &str) -> Result<()>;
    fn reset(&mut self) -> Result<()>;
    fn remove_all(&mut self) -> Result<()>;
    fn size(&self) -> Result<usize>;
    fn contains(&self, key: &str) -> Result<bool>;
    fn has(&self, key: &str) -> Result<bool>;
}
