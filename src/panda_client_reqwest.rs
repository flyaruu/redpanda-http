use reqwest::{blocking::Client, header::{HeaderMap, HeaderValue, CONTENT_TYPE, HeaderName}};

use crate::panda_client::{RedPandaHttpClient, RedPandaError};
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
    fn post(&mut self, url: &str, headers: &mut Vec<(String, String)>, body: Vec<u8>)->Result<Vec<u8>,RedPandaError> {
        // let headers = HeaderMap::new();
        let mut header_map: HeaderMap = HeaderMap::new();
        for (key,value) in headers {
            header_map.append(HeaderName::from_bytes(key.as_bytes()).unwrap(), HeaderValue::from_bytes(value.as_bytes()).unwrap());
        }
        header_map.append(CONTENT_TYPE, HeaderValue::from_bytes("application/vnd.kafka.v2+json".as_bytes()).unwrap());
        let result = self.client.post(url)
            .headers(header_map)
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