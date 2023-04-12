use std::{collections::HashMap, str::from_utf8};

use serde::{Serialize, Deserialize};

const DEBUG: bool = true;

#[derive(Serialize)]
pub struct Consumer<'a> {
    format: &'a str,
    #[serde(rename="auto.offset.reset")]
    auto_offset_reset: &'a str,
    #[serde(rename="auto.commit.enable")]
    auto_commit_enable: &'a str,
    #[serde(rename="fetch.min.bytes")]
    fetch_min_bytes: &'a str,
    #[serde(rename="consumer.request.timeout.ms")]
    consumer_request_timeout_ms: &'a str,
}

#[derive(Debug)]
pub struct RedPandaError(pub String);
pub trait RedPandaHttpClient {
    // fn post(&mut self,  url: &str, headers: &mut Vec<(&str, &str)>, data: Vec<u8>)->Result<Vec<u8>,RedPandaError>;
    fn post(&mut self, url: &str, headers: &mut Vec<(String, String)>, data: Vec<u8>)->Result<Vec<u8>,RedPandaError>;

    fn get(&mut self, url: &str)->Result<Vec<u8>, RedPandaError>;
}



impl<'a> Consumer<'a> {
    fn create()->Consumer<'a> {
        Consumer { format: "json", auto_offset_reset: "earliest", auto_commit_enable: "false", fetch_min_bytes: "0", consumer_request_timeout_ms: "10000" }
    }
}

#[derive(Serialize)]
pub struct CommitState {
    topics: HashMap<String,HashMap<u16,u64>>
}
#[derive(Serialize)]
pub struct CommitPartition {
    topic: String,
    partition: u16,
    offset: u64
}

impl CommitState {
    pub fn create()->CommitState {
        CommitState { topics: HashMap::new() }
    }

    pub fn process_record(&mut self, record: &Record) {
        let topic = self.topics.get_mut(&record.topic);
        match topic {
            None => {
                let mut new_topic: HashMap::<u16,u64> = HashMap::new();
                new_topic.insert(record.partition, record.offset + 1);
                self.topics.insert(record.topic.to_owned(), new_topic);
            }
            Some(map) => {
                map.insert(record.partition, record.offset + 1);
            }
        }
    }

    pub fn partition_list(&self)->Vec<CommitPartition> {
        let mut result: Vec<CommitPartition> = vec![];
        for (topic,partitions) in &self.topics {
            for (partition,offset) in partitions.iter() {
                result.push(CommitPartition {topic : topic.clone(), partition: *partition, offset: *offset});
            }
        }
        result
    }
}
#[derive(Deserialize)]
#[derive(Clone)]
struct ConsumerResponse {
    instance_id: String,
    base_uri: String
}
#[derive(Serialize)]
struct SubscribeRequest<'a> {
    topics: Vec<&'a str>
}

#[derive(Deserialize,Serialize)]
pub struct Record {
    pub topic: String,
    #[serde(with="base64")]
    pub key: Vec<u8>,
    #[serde(with="base64")]
    pub value: Vec<u8>,
    pub partition: u16,
    pub offset: u64
    
}

#[derive(Clone)]
pub struct RedPandaClient {
    inital_url: String,
    group: String,
    consumer_response: Option<ConsumerResponse>,
}

impl RedPandaClient {
    pub fn new(http_client: &mut Box<dyn RedPandaHttpClient>, inital_url: &str, group: &str)->Result<RedPandaClient,RedPandaError> {
        let mut client = RedPandaClient { inital_url: inital_url.to_owned(), group: group.to_owned(), consumer_response: Option::None};
        let consumer = Consumer::create();
        let body = serde_json::to_vec(&consumer)
            .map_err(|_| RedPandaError("Error serializing JSON request".to_owned()))?
        ;
        let url = format!("{}{}", client.inital_url, client.group);
        let mut headers = vec![("Content-Type".to_owned(),"application/vnd.kafka.v2+json".to_owned())];
        if DEBUG {
            println!("Initializing using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&consumer).unwrap())
        }
        let result = http_client.post(&url, &mut headers, body)?;
        if DEBUG {
            println!("Result text:\n{}", from_utf8(result.as_slice()).map_err(|_| RedPandaError("Issues creating utf8".to_owned()))?);
        }
        client.consumer_response = Some(serde_json::from_slice(&result).map_err(|_| RedPandaError("Error parsing JSON Red Panda reply".to_owned()))?);
        Ok(client)
    }

    pub fn register_topic(&mut self, client: &mut Box<dyn RedPandaHttpClient>, topics: Vec<&str>) ->  Result<(), RedPandaError> {
        let subscr = SubscribeRequest{topics};
        let url = format!("{}/subscription",self.consumer_response.as_ref().unwrap().base_uri);
        let body = serde_json::to_vec(&subscr)
            .map_err(|_| RedPandaError("Error serializing subscription request".to_owned()))?;
        if DEBUG {
            println!("Registering topic using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&subscr).unwrap())
        }
        let _ = client.post(&url, &mut Default::default(), body);
        Ok(())
    }

    pub fn poll(&mut self, client: &mut Box<dyn RedPandaHttpClient>, bytecount: i32) -> Result<Vec<Record>,RedPandaError> {
        let url = format!("{}/records?timeout=10000&&max_bytes={}",self.consumer_response.as_ref().unwrap().base_uri,bytecount);
        if DEBUG {
            println!("Calling get from url: {}",url);
        }
        let records = client.get(&url)?;
        let parsed:Vec<Record> = serde_json::from_slice(&records).map_err(|_| RedPandaError("Error parsing polling response".to_owned()))?;
        if DEBUG {
            println!("Polled from url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&parsed).unwrap());
        }
        Ok(parsed)
    }
    
    
    pub fn commit_state(&mut self, client: &mut Box<dyn RedPandaHttpClient>, state: &CommitState) ->  Result<(), RedPandaError> {
        let partitions = state.partition_list();
        let commits = HashMap::from([("partitions".to_owned(),partitions)]);
        let body = serde_json::to_vec(&commits ).map_err(|_| RedPandaError("Error serializing commit state".to_owned()))?;
        // let value = from_utf8(&body).map_err(|_| RedPandaError("UTF8 error".to_owned()))?;
        let url = format!("{}/offsets",self.consumer_response.as_ref().unwrap().base_uri);
        if DEBUG {
            println!("Committing to url:{}\nBody:\n{}",url,serde_json::to_string_pretty(&commits).unwrap());
        }
        let _ = client.post(&url, &mut Default::default(), body);
        Ok(())
    }

    pub fn instance_id(&self) -> Option<String> {
        Some(self.consumer_response.as_ref().unwrap().instance_id.clone())
    }
}

mod base64 {
    use base64::Engine;
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        // base64::engine::GeneralPurpose
        // BASE64_STANDARD::encode
        let base64 = base64::prelude::BASE64_STANDARD.encode(v);
        String::serialize(&base64, s)
    }
    

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::prelude::BASE64_STANDARD.decode(base64.as_bytes())
            .map_err(|e| serde::de::Error::custom(e))
    }
}