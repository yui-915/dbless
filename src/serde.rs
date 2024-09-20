use anyhow::Result;
pub use serde::{de::DeserializeOwned, Serialize};

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = rmp_serde::Serializer::new(vec![]).with_struct_map();
    value.serialize(&mut serializer)?;
    Ok(serializer.into_inner())
}

pub fn deserialize<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    Ok(rmp_serde::from_slice(value)?)
}
