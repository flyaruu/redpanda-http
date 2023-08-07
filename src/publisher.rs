use base64::{prelude::BASE64_STANDARD, Engine};
use serde::{Deserialize, Serialize, ser::SerializeMap};
use simplehttp::simplehttp::SimpleHttpClient;

use crate::{base64_option, RedPandaError};


#[derive(Deserialize)]
pub struct PublishRecord {
    pub key: Option<Vec<u8>>,
    #[serde(with="base64_option")]
    pub value: Option<Vec<u8>>,
}

impl PublishRecord {
    pub fn from_string(message: String)->Self {
        Self { key: None, value: Some(message.into_bytes()) }
    }

    pub fn from_bytes(value: Option<&[u8]>)->Self {
        PublishRecord { key: None, value: value.map(|f|f.to_vec()) }
    }
}

#[derive(Serialize)]
pub struct PublishRecordList {
    pub records: Vec<PublishRecord>
}

impl PublishRecordList {
    pub fn from_string(message: String)->Self {
        PublishRecordList{records: vec![PublishRecord::from_string(message)]}
    } 
}

impl Serialize for PublishRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        let field_count = self.key.as_ref().map(|_|1).unwrap_or(0) + 
            self.value.as_ref().map(|_|1).unwrap_or(0);

        // todo!()
        let mut map = serializer.serialize_map(Some(field_count))?;
        if self.key.is_some() {
            map.serialize_entry("key",&BASE64_STANDARD.encode(self.key.as_ref().unwrap()))?;
        }
        if self.value.is_some() {
            map.serialize_entry("value",&BASE64_STANDARD.encode(self.value.as_ref().unwrap()))?;
        }
        map.end()
    }
}

pub struct Publisher {
    inital_url: String,
    client: Box<dyn SimpleHttpClient>,
}

impl Publisher {

    pub fn new(http_client: Box<dyn SimpleHttpClient>, inital_url: &str)->Self {
        Self {inital_url: inital_url.to_owned(), client: http_client}
    }

    // TODO: return list of OffsetRecordSent
    pub fn publish(&mut self, topic: String, record: PublishRecordList)->Result<(), RedPandaError> {
        let url = format!("{}topics/{}",self.inital_url,topic);
        let l = serde_json::to_vec(&record).map_err(|e| RedPandaError::nested("error serializing publish", Box::new(e)))?;
        let _reply = self.client.post(&url, &[("Content-Type","application/vnd.kafka.binary.v2+json")], &l[..])
            .map_err(|e| RedPandaError::nested("error publishing",Box::new(e)))?;
        Ok(())
    }
}