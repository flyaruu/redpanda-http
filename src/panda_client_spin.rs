use http::{HeaderName, HeaderValue, Method};
use crate::panda_client::{RedPandaHttpClient, RedPandaError};

pub struct RedPandaClientSpinClient {

}

impl RedPandaClientSpinClient {
    pub fn new_spin()->Box<dyn RedPandaHttpClient> {
        Box::new(RedPandaClientSpinClient{})
    }

    fn prepare_request( uri: &str, headers: &Vec<(String, String)>, body: Option<Vec<u8>>, method: Method)->Result<http::Request<Option<bytes::Bytes>>,RedPandaError> {
        let mut request_builder = http::Request::builder()
            .method(method)
            .uri(uri);
        for (header_key, header_value) in headers {
            request_builder = request_builder.header(HeaderName::from_bytes(header_key.as_bytes()).unwrap(), HeaderValue::from_bytes(header_value.as_bytes()).unwrap());
        }
        
        match body  {
            Some(b) => request_builder.body( Some(bytes::Bytes::from(b))),
            None => request_builder.body(None)
        }.map_err(|_| RedPandaError("Error sending body".to_owned()))
    }
}
impl RedPandaHttpClient for RedPandaClientSpinClient {

    fn post(&mut self, uri: &str, headers: &Vec<(String, String)>, body: Vec<u8>)->Result<Vec<u8>,RedPandaError> {
        println!("Posting to uri: {}",uri);
        let request = RedPandaClientSpinClient::prepare_request(uri,headers,Some(body),Method::POST)?;
        let mut res = spin_sdk::http::send(
            request
        ).expect("response error");
        let result = res.body_mut().take().unwrap();
        println!("Bytes: {}",result.len());
        Ok(result.to_vec())
    }

    fn get(&mut self, uri: &str, headers: &Vec<(String, String)>)->Result<Vec<u8>, RedPandaError> {
        let request = RedPandaClientSpinClient::prepare_request(uri, headers, None, Method::GET)?;
        let mut res = spin_sdk::http::send(request)
            .map_err(|_| RedPandaError("Error calling get".to_owned()))
        .unwrap();
        let result = res.body_mut().take().unwrap();
        Ok(result.to_vec())
    }

}

