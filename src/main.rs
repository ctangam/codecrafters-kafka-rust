#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

use bytes::{Buf, BufMut, Bytes, BytesMut};

struct Request {
    header: RequestHeader,
    body: RequestBody,
}

#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
    _tagged_fields: Option<Vec<i32>>,
}

impl From<&[u8]> for RequestHeader {
    fn from(buffer: &[u8]) -> Self {
        let mut reader = buffer;
        let request_api_key = reader.get_i16();
        let request_api_version = reader.get_i16();
        let correlation_id = reader.get_i32();
        let client_id = String::from("");

        Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _tagged_fields: None,
        }
    }
}

struct RequestBody {
}

struct Response {
    header: ResponseHeader,
    body: ResponseBody,
}

impl Into<Vec<u8>> for &Response {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&Into::<Vec<u8>>::into(&self.header)[..]);
        buffer.extend_from_slice(&Into::<Vec<u8>>::into(&self.body)[..]);
        buffer
    }
}

struct ResponseHeader {
    correlation_id: i32,
}

impl Into<Vec<u8>> for &ResponseHeader {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer
    }
}

enum ResponseBody {
    ApiVersion(ApiVersion),
}

impl Into<Vec<u8>> for &ResponseBody {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match self {
            ResponseBody::ApiVersion(api_version) => {
                buffer.extend_from_slice(&Into::<Vec<u8>>::into(api_version)[..]);
            }
        }
        buffer
    }
}

struct ApiVersion {
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
}

impl Into<Vec<u8>> for &ApiVersion {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.api_keys.iter().map(|api_key| Into::<Vec<u8>>::into(api_key)).collect::<Vec<Vec<u8>>>().concat());
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer
    }
}

struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl Into<Vec<u8>> for &ApiKey {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.api_key.to_be_bytes());
        buffer.extend_from_slice(&self.min_version.to_be_bytes());
        buffer.extend_from_slice(&self.max_version.to_be_bytes());
        buffer
    }
}



fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut request = [0; 1024];
                stream.read(&mut request).unwrap();
                let length = u32::from_be_bytes(request[0..4].try_into().unwrap());
                let request: &RequestHeader = &request[4..].into();
                println!("request: {:?}", request);
                let error_code = match request.request_api_version {
                    0..=4 => 0,
                    _ => 35,
                };
                let response = Response {
                    header: ResponseHeader {
                        correlation_id: request.correlation_id,
                    },
                    body: ResponseBody::ApiVersion(ApiVersion {
                        error_code,
                        api_keys: vec![ApiKey {
                            api_key: request.request_api_key,
                            min_version: 0,
                            max_version: 4,
                        }],
                        throttle_time_ms: 0,
                    }),
                };
                let mut buffer = BytesMut::new();
                buffer.put_u32(0);
                let mut msg = buffer.split_off(4);
                msg.extend_from_slice(&Into::<Vec<u8>>::into(&response)[..]);
                buffer.copy_from_slice(&(msg.len() as u32).to_be_bytes());
                buffer.unsplit(msg);
                println!("buffer: {:?}", buffer.to_vec());
                
                stream.write(&buffer).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}