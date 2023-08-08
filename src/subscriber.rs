
use std::{collections::HashMap, str::from_utf8, vec};
use log::{debug, info, warn};
use serde::{Serialize, Deserialize};

const DEBUG: bool = true;

use simplehttp::simplehttp::SimpleHttpClient;

use crate::{base64_option, RedPandaError};

#[derive(Serialize)]
pub enum AutoOffsetReset {
    #[serde(rename="earliest")]
    Earliest,
    #[serde(rename="latest")]
    Latest,
}

#[derive(Serialize)]
pub struct Consumer<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    format: &'a str,
    #[serde(rename="auto.commit.enable")]
    auto_commit_enable: &'a str,
    #[serde(rename="fetch.min.bytes")]
    fetch_min_bytes: &'a str,
    #[serde(rename="consumer.request.timeout.ms")]
    consumer_request_timeout_ms: &'a str,
    #[serde(rename="auto.offset.reset")]
    auto_offset_reset: AutoOffsetReset,
}

impl<'a> Consumer<'a> {
    fn create(name: Option<&'a str>)->Consumer<'a> {
        Consumer { name, format: "json", auto_offset_reset: AutoOffsetReset::Earliest, auto_commit_enable: "false", fetch_min_bytes: "1", consumer_request_timeout_ms: "10000" }
    }
}

#[derive(Serialize,Deserialize)]
struct ReceivedOffset {
    topic: String,
    partition: u16,
    offset: i64,
}
#[derive(Serialize,Deserialize)]
struct ReceivedOffsets {
    offsets: Vec<ReceivedOffset>,
}

#[derive(Serialize,Deserialize)]
struct PartitionDefinition {
    topic: String,
    partition: u16,
}
#[derive(Serialize,Deserialize)]
struct PartitionDefinitions {
    partitions: Vec<PartitionDefinition>,
}


#[derive(Serialize,Debug)]
struct CommitState {
    topics: HashMap<String,HashMap<u16,i64>>
}
#[derive(Serialize,Debug)]
pub struct CommitPartition {
    topic: String,
    partition: u16,
    offset: i64
}

impl CommitState {
    fn create()->CommitState {
        CommitState { topics: HashMap::new() }
    }

    fn read_offset(&mut self, topic: String, partition: u16, offset: i64) {
        let topic = self.topics.entry(topic).or_insert(HashMap::new());
        topic.insert(partition,offset);
    }

    fn process_record(&mut self, record: &Record) {
        let topic = self.topics.get_mut(&record.topic);
        match topic {
            None => {
                let mut new_topic: HashMap::<u16,i64> = HashMap::new();
                new_topic.insert(record.partition, record.offset + 1);
                self.topics.insert(record.topic.to_owned(), new_topic);
            }
            Some(map) => {
                map.insert(record.partition, record.offset + 1);
            }
        }
    }

    // TODO rewrite as flat_map
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
#[derive(Clone,Debug)]
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
    pub offset: i64
}

pub struct Subscriber {
    inital_url: String,
    group: String,
    consumer_response: Option<ConsumerResponse>,
    client: Box<dyn SimpleHttpClient>,
    commit_state: CommitState,
}

impl From<(&str,u16)> for PartitionDefinition {
    fn from((topic,partition): (&str,u16)) -> Self {
        Self { topic: topic.to_owned(), partition}
    }
}

impl From<Vec<(&str,u16)>> for PartitionDefinitions {
    fn from(value: Vec<(&str,u16)>) -> Self {
        PartitionDefinitions { partitions: value.into_iter().map(|e| e.into()).collect() }
    }
}

impl Subscriber {
    pub fn new(http_client: Box<dyn SimpleHttpClient>, inital_url: &str, group: &str, name: Option<&str>)->Result<Self,RedPandaError> {
        let mut client = Self {inital_url: inital_url.to_owned(), group: group.to_owned(), consumer_response: Option::None, client: http_client, commit_state: CommitState::create()};
        let consumer = Consumer::create(name);
        let body = serde_json::to_vec(&consumer)
            .map_err(|e| RedPandaError::nested("Error serializing JSON request",Box::new(e)))?
        ;
        let url = format!("{}consumers/{}", client.inital_url, client.group);
        let headers = vec![("Content-Type","application/vnd.kafka.v2+json")];
        if DEBUG {
            debug!("Initializing using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&consumer).unwrap());
            debug!("Headers: {:?}",headers);
        }
        let result = client.client.post(&url, &headers, &body)
            .map_err(|e| RedPandaError::nested("error creating consumer",Box::new(e)))?;
        if DEBUG {
            debug!("Result text:\n{}", from_utf8(result.as_slice())
                .map_err(|e| RedPandaError::nested("Issues creating utf8",Box::new(e)))?);
        }

        client.consumer_response = Some(serde_json::from_slice(&result)
            .map_err(|e| RedPandaError::nested("Error parsing JSON Red Panda reply",Box::new(e)))?);
        debug!("Create consumer:\n{:?}",client.consumer_response);
        // client.commit_state()?;
        Ok(client)
    }

    pub fn register_topic(&mut self, topics: Vec<(&str,u16)>) ->  Result<(), RedPandaError> {
        let tlist: Vec<&str> = topics.iter()
            .map(|(topic,_)|(*topic).clone())
            .collect();
        let subscr = SubscribeRequest{topics: tlist};
        let url = format!("{}/subscription",self.consumer_response.as_ref().unwrap().base_uri);
        let body = serde_json::to_vec(&subscr)
            .map_err(|e| RedPandaError::nested("Error serializing subscription request",Box::new(e)))?;
        if DEBUG {
            debug!("Registering topic using url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&subscr).unwrap())
        }
        let _ = self.client.post(&url, &[("Content-Type","application/vnd.kafka.v2+json")], &body)
            .map_err(|e| RedPandaError::nested(&format!("error registering topic to: {:?}",url),Box::new(e)))?;

        let l = topics.iter()
            .flat_map(|(topic,partition_count)| (0..*partition_count).into_iter().map(|partition_number| (*topic,partition_number)) )
            .collect();
        self.create_commit_state(l)?;
        // self.commit_state = CommitState::create();
        self.commit_state()?;
        Ok(())
    }

    pub fn process_record(&mut self, record: &Record) {
        self.commit_state.process_record(record);

        debug!("Current commit state:\n{:?}",self.commit_state);
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
            .map_err(|e| RedPandaError::nested("error polling",Box::new(e)))?;
        if DEBUG {
            let text = String::from_utf8(records.clone()).unwrap();
            warn!("Result poll body: {}",text);
        }
        let parsed:Vec<Record> = serde_json::from_slice(&records)
            .map_err(|e|{RedPandaError::nested(&format!("Error parsing polling response. Response:\n{}",from_utf8(&records).unwrap_or("error")),Box::new(e))})?;
        if DEBUG {
            debug!("Polled from url: {}\nBody:\n{}",url,serde_json::to_string_pretty(&parsed).unwrap());
        }
        Ok(parsed)
    }
    
    fn create_commit_state(&mut self, offsets: Vec<(&str,u16)>) ->  Result<(), RedPandaError> {
        // http://localhost:8082/consumers/test_group/instances/test_consumer/offsets
        let base_uri = self.consumer_response.as_ref().unwrap().base_uri.as_str();
        // let group = self.group;
        debug!("BaseUrl: {}",base_uri);
        // let instance = self.consumer_response.as_ref().unwrap().instance_id.as_str();
        let url = format!("{}/offsets",base_uri);
        let headers = [("Accept", "application/vnd.kafka.v2+json"),("Content-Type", "application/vnd.kafka.v2+json")];

        let initial_offsets: PartitionDefinitions = offsets.into();
        let body = serde_json::to_vec_pretty(&initial_offsets).map_err(|e| RedPandaError::nested("Error serializing initial offsets",Box::new(e)))?;
        warn!("Get offsets Url: {}",url);
        warn!("Get offsets Body: {}",from_utf8(&body).unwrap());
        let res = self.client.get_with_body(&url, &headers, &body)
            .map_err(|e| RedPandaError::nested("error reading state",Box::new(e)))?;
        if DEBUG {
            warn!("Get offsets Result:\n{}",from_utf8(&res).unwrap())
        }
    

        let received_offsets: ReceivedOffsets = serde_json::from_slice(&res)
            .map_err(|e| RedPandaError::nested("error reading state", Box::new(e)))?;
        let mut commit_state = CommitState::create();
        received_offsets.offsets.into_iter().for_each(|element| {
            // floor the offset to 0. It appears to return -1 for new groups
            commit_state.read_offset(element.topic, element.partition, element.offset.max(0))

        });
        self.commit_state = commit_state;
        Ok(())
        // for ofset in received_offsets


    }

    pub fn commit_state(&mut self) ->  Result<(), RedPandaError> {
        let partitions = self.commit_state.partition_list();
        info!("Committing topics: {:?}",&partitions);
        let commits = HashMap::from([("partitions".to_owned(),partitions)]);
        let body = serde_json::to_vec(&commits ).map_err(|e| RedPandaError::nested("Error serializing commit state",Box::new(e)))?;
        // let value = from_utf8(&body).map_err(|_| RedPandaError("UTF8 error".to_owned()))?;
        let url = format!("{}/offsets",self.consumer_response.as_ref().unwrap().base_uri);
        if DEBUG {
            warn!("Committing to url:{}\nBody:\n{}",url,serde_json::to_string_pretty(&commits).unwrap());
        }
        let _result = self.client.post(&url, &[("Accept","application/vnd.kafka.v2+json"),("Content-Type","application/vnd.kafka.v2+json")], &body)
            .map_err(|e| RedPandaError::nested("error commiting state",Box::new(e)))?
        ;
        // I guess clear the commit state?
        self.commit_state.clear();
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