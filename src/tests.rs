use crate::{Database, TableReadInterface, TableWriteInterface};
use anyhow::Result;
use ctor::{ctor as run_before_tests, dtor as run_after_tests};
use serde::{Deserialize, Serialize};

type TestResult = Result<()>;

const TEST_DB_NAME: &str = "test.db";
static mut DB: Option<Database> = None;
static mut MEM: Option<Database> = None;

#[run_before_tests]
fn init_tests() {
    delete_test_db();
    unsafe {
        DB = Some(Database::open(TEST_DB_NAME).unwrap());
        MEM = Some(Database::in_memory().unwrap());
    }
}

macro_rules! test_db_and_tables {
    (|$db:ident| $block:block) => {{
        #[allow(unused_mut)]
        let mut $db = unsafe { DB.as_mut().unwrap() };
        $db.delete_all_tables()?;
        $block

        let t1l;
        {
            #[allow(unused_mut)]
            let mut $db = $db.table_mut("t1");
            assert_eq!($db.len()?, 0);
            $block
            t1l = $db.len()?;
        }

        let t2l;
        {
            #[allow(unused_mut)]
            let mut $db = $db.table_mut("t2");
            assert_eq!($db.len()?, 0);
            $block
            t2l = $db.len()?;
        }

        assert_eq!(t1l, t2l);

        #[allow(unused_mut)]
        let mut $db = unsafe { MEM.as_mut().unwrap() };
        $db.delete_all_tables()?;
        $block

        let t1l;
        {
            #[allow(unused_mut)]
            let mut $db = $db.table_mut("t1");
            assert_eq!($db.len()?, 0);
            $block
            t1l = $db.len()?;
        }

        let t2l;
        {
            #[allow(unused_mut)]
            let mut $db = $db.table_mut("t2");
            assert_eq!($db.len()?, 0);
            $block
            t2l = $db.len()?;
        }

        assert_eq!(t1l, t2l);

        Ok(())
    }};
}

#[test]
fn clear_len_is_empty() -> TestResult {
    test_db_and_tables!(|db| {
        db.set("key2", &"value")?; // overwritten 2 lines later
        db.set("key1", &"value")?;
        db.set("key2", &1234)?;
        db.set("key3", &"value")?;

        assert!(!db.is_empty()?);
        assert_eq!(db.len()?, 3);
        assert_eq!(db.size()?, 3);
        assert_eq!(db.keys()?.len(), 3);
        assert_eq!(db.values::<String>()?.len(), 2);
        assert_eq!(db.entries::<String>()?.len(), 2);
        assert_eq!(db.values::<i32>()?.len(), 1);
        assert_eq!(db.entries::<i32>()?.len(), 1);

        db.clear()?;

        assert!(db.is_empty()?);
        assert_eq!(db.len()?, 0);
        assert_eq!(db.size()?, 0);
        assert_eq!(db.keys()?.len(), 0);
        assert_eq!(db.values::<String>()?.len(), 0);
        assert_eq!(db.entries::<String>()?.len(), 0);
        assert_eq!(db.values::<i32>()?.len(), 0);
        assert_eq!(db.entries::<i32>()?.len(), 0);
    })
}

#[test]
fn get_set() -> TestResult {
    test_db_and_tables!(|db| {
        assert!(db.get::<String>("key")?.is_none());
        db.set("key", &"value")?;
        assert_eq!(db.get::<String>("key")?.unwrap(), "value".to_owned());
        assert!(db.get::<String>("key2")?.is_none());
        db.set("key2", &"value2")?;
        assert_eq!(db.get::<String>("key2")?.unwrap(), "value2".to_owned());
        assert_eq!(db.get::<String>("key")?.unwrap(), "value".to_owned());
    })
}

#[test]
fn remove() -> TestResult {
    test_db_and_tables!(|db| {
        db.set("key", &"value")?;
        assert_eq!(db.get::<String>("key")?.unwrap(), "value".to_owned());
        db.remove("key")?;
        assert!(db.get::<String>("key")?.is_none());
        db.remove("key")?; // should not error
    })
}

#[test]
fn contains() -> TestResult {
    test_db_and_tables!(|db| {
        assert!(!db.contains("key")?);
        assert!(!db.contains_key("key")?);
        assert!(!db.has("key")?);
        db.set("key", &"value")?;
        assert!(db.contains("key")?);
        assert!(db.contains_key("key")?);
        assert!(db.has("key")?);
        db.remove("key")?;
        assert!(!db.contains("key")?);
        assert!(!db.contains_key("key")?);
        assert!(!db.has("key")?);
    })
}

#[test]
fn keys_values_entries() -> TestResult {
    test_db_and_tables!(|db| {
        assert_eq!(db.keys()?.len(), 0);
        assert_eq!(db.values::<String>()?.len(), 0);
        assert_eq!(db.entries::<String>()?.len(), 0);

        db.set("key", &"value")?;

        let expected = vec!["key".to_owned()];
        let reality = db.keys()?;
        assert_eq!(expected, reality);

        let expected = vec!["value".to_owned()];
        let reality = db.values::<String>()?;
        assert_eq!(expected, reality);

        let expected = vec![("key".to_owned(), "value".to_owned())];
        let reality = db.entries::<String>()?;
        assert_eq!(expected, reality);

        let expected = vec![("key".to_owned(), "value".to_owned())];
        let reality = db.entries()?;
        assert_eq!(expected, reality);

        db.set("key2", &"value2")?;
        db.set("key3", &12345)?;

        let mut expected = vec!["key".to_owned(), "key2".to_owned(), "key3".to_owned()];
        let mut reality = db.keys()?;
        expected.sort();
        reality.sort();
        assert_eq!(expected, reality);

        let mut expected = vec!["value".to_owned(), "value2".to_owned()];
        let mut reality = db.values::<String>()?;
        expected.sort();
        reality.sort();
        assert_eq!(expected, reality);

        let mut expected = vec![
            ("key".to_owned(), "value".to_owned()),
            ("key2".to_owned(), "value2".to_owned()),
        ];
        let mut reality = db.entries::<String>()?;
        expected.sort();
        reality.sort();
        assert_eq!(expected, reality);

        let expected = vec![12345];
        let reality = db.values::<i32>()?;
        assert_eq!(expected, reality);

        let expected = vec![("key3".to_owned(), 12345)];
        let reality = db.entries::<i32>()?;
        assert_eq!(expected, reality);
    })
}

#[derive(Serialize, Deserialize)]
struct UserV1 {
    name: String,
    pass: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct UserV2 {
    #[serde(rename = "name")]
    username: String,
    #[serde(default = "exiting_user_default_role")]
    role: Role,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
enum Role {
    Admin,
    Registered,
    #[default]
    Guest,
}

fn exiting_user_default_role() -> Role {
    Role::Registered
}

#[test]
fn serde() -> TestResult {
    test_db_and_tables!(|db| {
        db.set(
            "user1",
            &UserV1 {
                name: "yui-915".to_owned(),
                pass: "123456".to_owned(),
            },
        )?;

        db.set(
            "user2",
            &UserV2 {
                username: "guest-1273".to_owned(),
                role: Default::default(),
            },
        )?;

        assert_eq!(
            db.get::<UserV2>("user1")?.unwrap(),
            UserV2 {
                username: "yui-915".to_owned(),
                role: Role::Registered,
            }
        );

        assert_eq!(
            db.get::<UserV2>("user2")?.unwrap(),
            UserV2 {
                username: "guest-1273".to_owned(),
                role: Role::Guest,
            }
        );
    })
}

#[run_after_tests]
fn delete_test_db() {
    let _ = std::fs::remove_file(TEST_DB_NAME);
}
