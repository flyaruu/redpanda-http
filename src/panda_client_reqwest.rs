use reqwest::blocking::Client;

use crate::panda_client::{RedPandaHttpClient, RedPandaError, RedPandaClient};


// pub fn new_reqwest_panda_client(initial_url: &str, group: &str)->Result<RedPandaClient,RedPandaError> {
//     RedPandaClient::new(initial_url, group)
// }
pub struct ReqwestRedPandaClient {
    client: Client
}

impl ReqwestRedPandaClient {
    pub fn new_reqwest()->Box<dyn RedPandaHttpClient> {
        Box::new(ReqwestRedPandaClient { client: Client::builder().build().unwrap()})
    }
}
impl RedPandaHttpClient for ReqwestRedPandaClient {
    fn post(&mut self, url: &str, body: Vec<u8>)->Result<Vec<u8>,RedPandaError> {
        let result = self.client.post(url)
            .header("Content-Type", "application/vnd.kafka.v2+json")
            .body(body)
            .send()
            .map_err(|e| RedPandaError(e.to_string()))?
            .bytes()
            .map_err(|_| RedPandaError("Encoding problem".to_owned()))?
            .to_vec();
        Ok(result)
    }

    fn get(&mut self, url: &str)->Result<Vec<u8>, RedPandaError> {
        let result = self.client.get(url)
            .header("Accept", "application/vnd.kafka.binary.v2+json")
            .send()
            .map_err(|_| RedPandaError("Error polling".to_owned()))?
            .bytes()
            .map_err(|_| RedPandaError("Encoding problem?".to_owned()))?
            .to_vec();
        Ok(result)
        
    }
}