# dbless

A simple key-value store for rust apps that don't need a full-flagged database.

heavily inspired be [bevy_pkv](https://crates.io/crates/bevy_pkv),
based on [redb](https://crates.io/crates/redb) and [rmp-serde](https://crates.io/crates/rmp-serde).

## Features

- Simple and easy to use, with mininal boilerplate.
- Works with any type that implement `serde::Serialize` and `serde::Deserialize`.
- Has an in-memory backend if the data doesn't need to be saved to disk.
- Multiple tables support.

## Examples

Hello world

```rust
use dbless::Database;

// traits needed to use set and get methods
use dbless::{TableReadInterface, TableWriteInterface};


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::open("my_database.db")?;

    db.set("key", &"Hello, world!")?;
    let value: Option<String> = db.get("key")?;
    db.remove("key")?;
    println!("{:?}", value);

    Ok(())
}
```

Using with types that implement `serde::Serialize` and `serde::Deserialize`

```rust
use dbless::{Database, TableReadInterface, TableWriteInterface};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    username: String,
    role: Role,
}

#[derive(Serialize, Deserialize, Debug)]
enum Role {
    Admin,
    User,
    Guest,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // temporary, in-memory database instead of a file
    let mut db = Database::in_memory()?;

    db.set("user1", &User {
        username: "admin-1".to_string(),
        role: Role::Admin,
    })?;

    db.set("user2", &User {
        username: "user-69".to_string(),
        role: Role::User,
    })?;

    db.set("user3", &User {
        username: "guest-420".to_string(),
        role: Role::Guest,
    })?;

    let users: Vec<User> = db.values()?;
    println!("{:?}", users);

    Ok(())
}
```

Multiple tables

```rust
use dbless::{Database, TableReadInterface, TableWriteInterface};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::in_memory()?;

    db.set("msg", &"Hello from main table")?;
    db.table_mut("table1").set("msg", &"Hello from table 1")?;
    db.table_mut("table2").set("msg", &"Hello from table 2")?;

    let msg1 = db.get::<String>("msg")?;
    println!("{:?}", msg1);

    let msg2 = db.table("table1").get::<String>("msg")?;
    println!("{:?}", msg2);

    let msg3 = db.table("table2").get::<String>("msg")?;
    println!("{:?}", msg3);

    Ok(())
}
```

For all methods with their documentation/examples, check:
- [`Database`](https://docs.rs/dbless/latest/dbless/struct.Database.html).
- [`TableReadInterface`](https://docs.rs/dbless/latest/dbless/trait.TableReadInterface.html).
- [`TableWriteInterface`](https://docs.rs/dbless/latest/dbless/trait.TableWriteInterface.html).

### About the default table
Using methods from [`TableReadInterface`](trait.TableReadInterface.html) and [`TableWriteInterface`](trait.TableWriteInterface.html) directly on [`Database`](struct.Database.html) \
uses a default table named `#_#_main_dbless_table_#_#`.

calling [`clear()`](struct.Database.html#method.clear) or [`reset()`](struct.Database.html#method.reset) will only clear this table, not the entire database, \
to clear the entire database, use [`delete_all_tables()`](struct.Database.html#method.delete_all_tables)

similarly, calling [`len()`](struct.Database.html#method.len) or [`size()`](struct.Database.html#method.size) will only count the number of entries in this table, \
to count the number of entries in the entire database, use [`len_all_tables()`](struct.Database.html#method.len_all_tables) or [`size_all_tables()`](struct.Database.html#method.size_all_tables).

---

License: MIT OR Apache-2.0
