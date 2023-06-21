use std::str::from_utf8;

use reqwest::{blocking::Client, header::{HeaderMap, HeaderValue, HeaderName}, Method};
use crate::panda_client::{RedPandaHttpClient, RedPandaError};
pub struct ReqwestRedPandaClient {
    client: Client
}

impl ReqwestRedPandaClient {
    pub fn new_reqwest()->Box<dyn RedPandaHttpClient> {
        Box::new(ReqwestRedPandaClient { client: Client::builder().build().unwrap()})
    }

    pub fn prepare_request(&self, url: &str, headers: &Vec<(String, String)>, body: Option<Vec<u8>>, method: reqwest::Method)->Result<reqwest::blocking::Request, RedPandaError> {
        let mut header_map: HeaderMap = HeaderMap::new();
        for (key,value) in headers {
            header_map.append(HeaderName::from_bytes(key.as_bytes()).unwrap(), HeaderValue::from_bytes(value.as_bytes()).unwrap());
        }
        let builder = self.client
            .request(method, url)
            .headers(header_map);

        let builder = match body {
            Some(b) => builder.body(b),
            None => builder,
        };
        builder.build().map_err(|_| RedPandaError("Error creating request".to_owned()))
    }

}
impl RedPandaHttpClient for ReqwestRedPandaClient {
    fn post(&mut self, url: &str, headers: &Vec<(String, String)>, body: Vec<u8>)->Result<Vec<u8>,RedPandaError> {
        let request = self.prepare_request(url, &headers, Some(body), Method::POST)?;
        let response = self.client.execute(request)
            .map_err(|_| RedPandaError("Error sending post".to_owned()))?;
        
        let response_status = response.status();
        let response_body = response.bytes()
            .map_err(|_| RedPandaError("Error decoding post response".to_owned()))?
            .to_vec();
        if !response_status.is_success() {
            return Err(RedPandaError(format!("Error status code: {}\n body: {}",response_status.as_u16(), from_utf8(&response_body).unwrap())))
        }            
        Ok(response_body)        

    }

    fn get(&mut self, url: &str, headers: &Vec<(String, String)>)->Result<Vec<u8>, RedPandaError> {
        let request = self.prepare_request(url, &headers, None, Method::GET)?;
        let result = self.client.execute(request)
            .map_err(|_| RedPandaError("Error sending post".to_owned()))?
            .bytes()
            .map_err(|_| RedPandaError("Error decoding post response".to_owned()))?
            .to_vec();
        Ok(result)
    }
}