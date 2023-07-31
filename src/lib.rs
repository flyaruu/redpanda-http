pub mod panda_subscriber;
pub mod panda_publisher;

use std::fmt::Display;

use thiserror::Error;

#[derive(Debug,Error)]
pub struct RedPandaError(pub String);

impl Display for RedPandaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

mod base64_option {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        // base64::encode(input)
        let base64 = v.as_ref().map(|element| BASE64_STANDARD.encode(element));
        // let base64 = match v {

        //     Some(v) => Some(BASE64_STANDARD.encode(v)),
        //     None => None,
        // };
        <Option<String>>::serialize(&base64, s)
    }
    
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let base64 = <Option<String>>::deserialize(d)?;
        match base64 {
            Some(v) => {
                BASE64_STANDARD.decode(v.as_bytes())
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            },
            None => Ok(None),
        }
    }
}
