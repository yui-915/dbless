use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

use crate::store::Store;

/// A trait for reading from a table
pub trait TableReadInterface {
    /// Gets the value associated with the given key.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value1: Option<String> = db.get("key")?;
    /// let value2 = db.get::<String>("key2")?;
    /// println!("got values: {:?}, {:?}", value1, value2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;

    /// Gets a list of all keys in the table.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let keys = db.keys()?;
    /// for key in keys {
    ///     println!("{}", key);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn keys(&self) -> Result<Vec<String>>;

    /// Gets a list of all values in the table (that can be deserialized into the given type).
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let values = db.values::<String>()?;
    /// for value in values {
    ///     println!("{}", value);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>>;

    /// Gets a list of all entries in the table (that can be deserialized into the given type).
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let entries = db.entries::<i32>()?;
    /// for (key, value) in entries {
    ///     println!("{}: {}", key, value);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>>;

    /// Gets the number of entries in the table. \
    /// aliases: [`size()`](#method.size)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let len = db.len()?;
    /// println!("the default table has {} entries", len);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn len(&self) -> Result<usize>;

    /// Checks if the table contains the given key. \
    /// aliases: [`contains()`](#method.contains), [`has()`](#method.has)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let contains = db.contains_key("key")?;
    /// if contains {
    ///     println!("the default table contains the key");
    /// } else {
    ///     println!("the default table does not contain the key");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn contains_key(&self, key: &str) -> Result<bool>;

    /// Checks if the table is empty.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let is_empty = db.is_empty()?;
    /// if is_empty {
    ///     println!("the default table is empty");
    /// } else {
    ///     println!("the default table is not empty");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn is_empty(&self) -> Result<bool>;

    /// Gets the number of entries in the table. \
    /// aliases: [`len()`](#method.len)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let size = db.size()?;
    /// println!("the default table has {} entries", size);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn size(&self) -> Result<usize>;

    /// Checks if the table contains the given key. \
    /// aliases: [`contains_key()`](#method.contains_key), [`has()`](#method.has)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let contains = db.contains("key")?;
    /// if contains {
    ///     println!("the default table contains the key");
    /// } else {
    ///     println!("the default table does not contain the key");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn contains(&self, key: &str) -> Result<bool>;

    /// Checks if the table contains the given key. \
    /// aliases: [`contains()`](#method.contains), [`has()`](#method.has)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let has = db.has("key")?;
    /// if has {
    ///     println!("the default table contains the key");
    /// } else {
    ///     println!("the default table does not contain the key");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn has(&self, key: &str) -> Result<bool>;

    /// Gets the value associated with the given key, \
    /// if an error occurs, or no value is found, returns the given default value.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value = db.get_or("my_number", 69)?;
    /// println!("got nice number maybe: {}", value);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or<T: DeserializeOwned>(&self, key: &str, default: T) -> Result<T>;

    /// Gets the value associated with the given key, \
    /// if an error occurs, or no value is found, calls the given closure and returns the result.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value = db.get_or_else("my_number", || {
    ///     // some expensive calculation that needs to be done lazily
    ///     20 + 400
    /// })?;
    /// println!("got not as nice number maybe: {}", value);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or_else<T: DeserializeOwned, F: FnOnce() -> T>(
        &self,
        key: &str,
        default: F,
    ) -> Result<T>;

    /// Gets the value associated with the given key, \
    /// if an error occurs, or no value is found, returns the default value for the given type.
    /// if no value is found, returns the default value for the given type.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// let db = Database::open("my_database.db")?;
    /// let value = db.get_or_default::<i32>("my_number")?;
    /// println!("got zero maybe: {}", value);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> Result<T>;
}

/// A trait for writing to a table
pub trait TableWriteInterface {
    /// Inserts a value into the table with the given key. \
    /// aliases: [`set()`](#method.set)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.insert("key", &"value")?;
    /// db.insert("key2", &1234)?;
    /// db.insert("key3", &true)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn insert<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;

    /// Removes the value associated with the given key. \
    /// aliases: [`delete()`](#method.delete)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.remove("key")?;
    /// assert!(!db.contains_key("key")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn remove(&mut self, key: &str) -> Result<()>;

    /// Clears the table. \
    /// aliases: [`reset()`](#method.reset)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.clear()?;
    /// assert!(db.is_empty()?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn clear(&mut self) -> Result<()>;

    /// Inserts a value into the table with the given key. \
    /// aliases: [`insert()`](#method.insert)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.set("key", &"value")?;
    /// db.set("key2", &1234)?;
    /// db.set("key3", &true)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn set<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;

    /// Removes the value associated with the given key. \
    /// aliases: [`remove()`](#method.remove)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.delete("key")?;
    /// assert!(!db.contains_key("key")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn delete(&mut self, key: &str) -> Result<()>;

    /// Clears the table. \
    /// aliases: [`clear()`](#method.clear)
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// db.reset()?;
    /// assert!(db.is_empty()?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn reset(&mut self) -> Result<()>;

    /// Gets the value associated with the given key, \
    /// if the no value is found, inserts the given default value into the table and returns it.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// let value = db.get_or_insert("key", "default".to_owned())?;
    /// println!("got default maybe: {}", value);
    /// assert!(db.contains_key("key")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or_insert<T: Serialize + DeserializeOwned>(
        &mut self,
        key: &str,
        default: T,
    ) -> Result<T>;

    /// Gets the value associated with the given key, \
    /// if the no value is found, inserts the result of the given closure into the table and returns it.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// let value = db.get_or_insert_with("key", || {
    ///     // some expensive calculation that needs to be done lazily
    ///     "Hello, world!".to_owned()
    /// })?;
    /// println!("got hello maybe: {}", value);
    /// assert!(db.contains_key("key")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or_insert_with<T: Serialize + DeserializeOwned, F: FnOnce() -> T>(
        &mut self,
        key: &str,
        default: F,
    ) -> Result<T>;

    /// Gets the value associated with the given key, \
    /// if the no value is found, inserts the default value for the given type into the table and returns it.
    /// ```no_run
    /// # use dbless::Database;
    /// # use dbless::TableReadInterface;
    /// # use dbless::TableWriteInterface;
    /// let mut db = Database::open("my_database.db")?;
    /// let value = db.get_or_insert_default::<i32>("key")?;
    /// println!("got zero maybe: {}", value);
    /// assert!(db.contains_key("key")?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_or_insert_default<T: Serialize + DeserializeOwned + Default>(
        &mut self,
        key: &str,
    ) -> Result<T>;
}

/// A read-only handle to a table
pub struct Table<'a> {
    pub(crate) store: &'a Store,
    pub(crate) name: &'a str,
}

/// A read-write handle to a table
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
        Ok(self.store.len(self.name)? == 0)
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

    fn get_or<T: DeserializeOwned>(&self, key: &str, default: T) -> Result<T> {
        Ok(self.get(key)?.unwrap_or(default))
    }

    fn get_or_else<T: DeserializeOwned, F: FnOnce() -> T>(
        &self,
        key: &str,
        default: F,
    ) -> Result<T> {
        Ok(self.get(key)?.unwrap_or_else(default))
    }

    fn get_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> Result<T> {
        self.get_or_else(key, T::default)
    }
}

macro_rules! mirror_methods_with_into {
    {$into:ident; $(fn $name:ident$(<$($gname:ident: $gty1:ident $(+$gtyr:ident)*),+>)?(&self $(,$pname:ident: $pty:ty)*) -> $ret:ty;)*} => {
        $(
            fn $name$(<$($gname: $gty1$(+$gtyr)*),+>)?(&self, $($pname: $pty),*) -> $ret {
                Into::<$into>::into(self).$name($($pname),*)
            }
        )*
    }
}

impl<'a> TableReadInterface for TableMut<'a> {
    mirror_methods_with_into! {
        Table;
        fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;
        fn keys(&self) -> Result<Vec<String>> ;
        fn values<T: DeserializeOwned>(&self) -> Result<Vec<T>> ;
        fn entries<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>> ;
        fn len(&self) -> Result<usize> ;
        fn is_empty(&self) -> Result<bool> ;
        fn contains_key(&self, key: &str) -> Result<bool> ;
        fn size(&self) -> Result<usize> ;
        fn contains(&self, key: &str) -> Result<bool> ;
        fn has(&self, key: &str) -> Result<bool> ;
        fn get_or<T: DeserializeOwned>(&self, key: &str, default: T) -> Result<T> ;
        fn get_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> Result<T> ;
    }

    // current macro can't handle FnOnce() -> T
    fn get_or_else<T: DeserializeOwned, F: FnOnce() -> T>(
        &self,
        key: &str,
        default: F,
    ) -> Result<T> {
        Into::<Table>::into(self).get_or_else(key, default)
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

    fn get_or_insert<T: Serialize + DeserializeOwned>(
        &mut self,
        key: &str,
        default: T,
    ) -> Result<T> {
        self.get_or_insert_with(key, move || default)
    }

    fn get_or_insert_with<T: Serialize + DeserializeOwned, F: FnOnce() -> T>(
        &mut self,
        key: &str,
        default: F,
    ) -> Result<T> {
        match self.get(key)? {
            Some(value) => Ok(value),
            None => {
                let default = default();
                self.insert(key, &default)?;
                Ok(default)
            }
        }
    }

    fn get_or_insert_default<T: Serialize + DeserializeOwned + Default>(
        &mut self,
        key: &str,
    ) -> Result<T> {
        self.get_or_insert_with(key, T::default)
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
