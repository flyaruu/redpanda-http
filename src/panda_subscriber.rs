
use std::{collections::HashMap, str::from_utf8, vec};
use log::debug;
use serde::{Serialize, Deserialize};

const DEBUG: bool = true;

use simplehttp::simplehttp::SimpleHttpClient;

use crate::{base64_option, RedPandaError};

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




impl<'a> Consumer<'a> {
    fn create()->Consumer<'a> {
        Consumer { format: "json", auto_offset_reset: "earliest", auto_commit_enable: "false", fetch_min_bytes: "0", consumer_request_timeout_ms: "10000" }
    }
}

#[derive(Serialize)]
struct CommitState {
    topics: HashMap<String,HashMap<u16,u64>>
}
#[derive(Serialize)]
pub struct CommitPartition {
    topic: String,
    partition: u16,
    offset: u64
}

impl CommitState {
    fn create()->CommitState {
        CommitState { topics: HashMap::new() }
    }

    fn process_record(&mut self, record: &Record) {
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

    fn partition_list(&self)->Vec<CommitPartition> {
        let mut result: Vec<CommitPartition> = vec![];
        for (topic,partitions) in &self.topics {
            for (partition,offset) in partitions.iter() {
                result.push(CommitPartition {topic : topic.clone(), partition: *partition, offset: *offset});
            }
        }
        result
    }

    fn clear(&mut self) {
        self.topics.clear()
    }
}
#[derive(Deserialize,Serialize)]
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
    #[serde(with="base64_option")]
    pub key: Option<Vec<u8>>,
    #[serde(with="base64_option")]
    pub value: Option<Vec<u8>>,
    pub partition: u16,
    pub offset: u64
}

pub struct Subscriber {
    inital_url: String,
    group: String,
    consumer_response: Option<ConsumerResponse>,
    client: Box<dyn SimpleHttpClient>,
    commit_state: CommitState,
}

impl Subscriber {
    pub fn new(http_client: Box<dyn SimpleHttpClient>, inital_url: &str, group: &str)->Result<Self,RedPandaError> {
        let mut client = Self {inital_url: inital_url.to_owned(), group: group.to_owned(), consumer_response: Option::None, client: http_client, commit_state: CommitState::create()};
        let consumer = Consumer::create();
        let body = serde_json::to_vec(&consumer)
            .map_err(|_| RedPandaError("Error serializing JSON request".to_owned()))?
        ;
        let url = format!("{}consumers/{}", client.inital_url, client.group);
        let headers = vec![("Content-Type","application/vnd.kafka.v2+json")];
        if DEBUG {
            debug!("Initializing using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&consumer).unwrap());
            debug!("Headers: {:?}",headers);
        }
        let result = client.client.post(&url, &headers, &body).map_err(|e| RedPandaError(format!("error creating consumer: {:?}",e)))?;
        if DEBUG {
            debug!("Result text:\n{}", from_utf8(result.as_slice()).map_err(|_| RedPandaError("Issues creating utf8".to_owned()))?);
        }

        client.consumer_response = Some(serde_json::from_slice(&result).map_err(|_| RedPandaError("Error parsing JSON Red Panda reply".to_owned()))?);

        Ok(client)
    }

    pub fn register_topic(&mut self, topics: Vec<&str>) ->  Result<(), RedPandaError> {
        let subscr = SubscribeRequest{topics};
        let url = format!("{}/subscription",self.consumer_response.as_ref().unwrap().base_uri);
        let body = serde_json::to_vec(&subscr)
            .map_err(|_| RedPandaError("Error serializing subscription request".to_owned()))?;
        if DEBUG {
            debug!("Registering topic using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&subscr).unwrap())
        }
        let _ = self.client.post(&url, &[("Content-Type","application/vnd.kafka.v2+json")], &body).map_err(|e| RedPandaError(format!("error registering topic: {:?}",e)))?;
        Ok(())
    }

    pub fn process_record(&mut self, record: &Record) {
        self.commit_state.process_record(record);
        self.commit_state.clear();
    }
    // pub fn poll_thing<'a, T>(&mut self, bytecount: i32) -> Result<Vec<T>,RedPandaError> where for<'de> T: Deserialize<'de> + 'a {
    //     let l: Vec<Record> = self.poll( bytecount)?;
    //     let xx: Vec<T> = l.iter().filter_map(f) map(|f| serde_json::from_slice(&f.value[..])).collect();
    // }

    pub fn poll(&mut self, bytecount: i32) -> Result<Vec<Record>,RedPandaError> {
        let url = format!("{}/records?timeout=10000&&max_bytes={}",self.consumer_response.as_ref().unwrap().base_uri,bytecount);
        if DEBUG {
            debug!("Calling get from url: {}",url);
        }
                // .header("Accept", "application/vnd.kafka.binary.v2+json")

        let records = self.client.get(&url,&[("Accept", "application/vnd.kafka.binary.v2+json")])
            .map_err(|e| RedPandaError(format!("error polling: {:?}",e)))?;
        if DEBUG {
            let text = String::from_utf8(records.clone()).unwrap();
            debug!("Result body: {}",text);
        }
        let parsed:Vec<Record> = serde_json::from_slice(&records).map_err(|_|{RedPandaError(format!("Error parsing polling response. Response:\n{}",from_utf8(&records).unwrap_or("error")))})?;
        if DEBUG {
            debug!("Polled from url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&parsed).unwrap());
        }
        Ok(parsed)
    }
    


    pub fn commit_state(&mut self) ->  Result<(), RedPandaError> {
        let partitions = self.commit_state.partition_list();
        let commits = HashMap::from([("partitions".to_owned(),partitions)]);
        let body = serde_json::to_vec(&commits ).map_err(|_| RedPandaError("Error serializing commit state".to_owned()))?;
        // let value = from_utf8(&body).map_err(|_| RedPandaError("UTF8 error".to_owned()))?;
        let url = format!("{}/offsets",self.consumer_response.as_ref().unwrap().base_uri);
        if DEBUG {
            debug!("Committing to url:{}\nBody:\n{}",url,serde_json::to_string_pretty(&commits).unwrap());
        }
        let _ = self.client.post(&url, &[], &body)
            .map_err(|e| RedPandaError(format!("error commiting state: {:?}",e)))
        ;
        // I guess clear the commit state?
        Ok(())
    }

    pub fn instance_id(&self) -> Option<String> {
        Some(self.consumer_response.as_ref().unwrap().instance_id.clone())
    }
}



#[cfg(test)]
#[cfg(feature = "reqwest")]
mod tests {

    #[test]
    fn test_reqwest() {
        
    }
}