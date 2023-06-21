

use std::str::from_utf8;

use embedded_svc::http::{client::*, Headers};
use embedded_svc::io::{Write, Read};
use esp_idf_svc::http::client::*;

use crate::panda_client::{RedPandaHttpClient, RedPandaError};

pub fn new_esp_http()->Box<dyn RedPandaHttpClient> {
    Box::new(EspRedPandaHttpClient::new().unwrap())
}

pub struct EspRedPandaHttpClient {
    client: Client<EspHttpConnection>
}

impl EspRedPandaHttpClient {
    pub fn new()->Result<EspRedPandaHttpClient,RedPandaError> {
        let client = Client::wrap(EspHttpConnection::new(&Configuration {
            crt_bundle_attach: Some(esp_idf_sys::esp_crt_bundle_attach),
            ..Default::default()
        }).map_err(|_| RedPandaError("Error creating http client".to_owned()))?);
        Ok(EspRedPandaHttpClient{client})
    }

    pub fn read_response(mut response: Response<&mut EspHttpConnection>)->Result<Vec<u8>,RedPandaError> {
        let size = response.content_len().ok_or(RedPandaError("Error reading content".to_owned()))? as usize;
        let mut body = [0_u8; 3048];
        let mut output_buffer: Vec<u8> = Vec::with_capacity(size);
        loop {
            match response.read(&mut body) {
                Ok(bytes_read) => {
                    // println!("Bytes read: {}",bytes_read);
                    if bytes_read>0 {
                        output_buffer.extend_from_slice(&body[0..bytes_read]);
                    } else {
                        // println!("Result:\n{}",from_utf8(&output_buffer).unwrap());
                        return Ok(output_buffer);
                    }
                },
                Err(_) => return Err(RedPandaError("Error reading content:".to_owned())),
            };
        }
    }
}

impl RedPandaHttpClient for EspRedPandaHttpClient {
    fn get(&mut self, url: &str, input_headers: &Vec<(String, String)>)->Result<Vec<u8>, RedPandaError> {
        // println!("Getting url: {}",url);
        let mut headers = input_headers.clone();
        headers.push(("Accept".to_owned(), "application/vnd.kafka.binary.v2+json".to_owned()));        
        let collected_headers: Vec<(&str,&str)> = headers.iter().map(|(k,v)|(k.as_ref(),v.as_ref())).collect();
        let response = self.client
            .request(Method::Get,&url,&collected_headers)
            .map_err(|e| RedPandaError(format!("Error createing  get: {}",e).to_owned()))?
            .submit()
            .map_err(|e| RedPandaError(format!("Error connecting: {}",e).to_owned()))?;
        Self::read_response(response)
    }

    fn post<'a>(&'a mut self, url: &str, input_headers: &Vec<(String, String)>, data: Vec<u8>)->Result<Vec<u8>,RedPandaError> {
        // println!("Posting url: {}",url);
        if url.contains("localhost") {
            println!("\n\n!!!! Do you really want to use localhost from esp? I doubt that'n'n")
        }
        let a = from_utf8(&data)
            .map_err(|_| RedPandaError("Error parsing body".to_owned()))?;
        // println!("Body: {}",a);
        let length_string = format!("{}",data.len());
        let mut headers = input_headers.clone();
        headers.push(("Content-Length".to_owned(),length_string));        
        let collected: Vec<(&str,&str)> = headers.iter().map(|(k,v)|(k.as_ref(),v.as_ref())).collect();
        // println!("Headers: {:?}",collected);
        let mut post_request = self.client
            .post(url,&collected)
            .map_err(|e| RedPandaError(format!("Error posting url: {:?}",e)))?;
        // post_request.flush()
        //     .map_err(|_| RedPandaError("Error flushing url".to_owned()))?;
        post_request.write_all(&data).map_err(|e| RedPandaError(format!("Error posting url: {:?}",e)))?;
        // loop {
        //     match post_request.write(&data) {
        //         Ok(write_count) => {
        //             println!("Wrote: {} bytes",write_count);
        //             post_request.flush().unwrap();
        //             break;
        //         },
        //         Err(e) => return {
        //             println!("ERror: {}",e);
        //             Err(RedPandaError("Error writing post".to_owned()))
        //         },
        //     }
        // }
        // post_request.write(&data)
        //     .map_err(|_| RedPandaError("Error getting url".to_owned()))?;
        // println!("Sent body");

        let post_response = post_request.submit()
                .map_err(|_| RedPandaError("Error sending data".to_owned()))?;
        Self::read_response(post_response)     
    }
}