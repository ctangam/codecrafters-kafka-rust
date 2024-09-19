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

struct ResponseHeader {
    correlation_id: i32,
}

struct ResponseBody {

}


fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut request = Vec::new();
                stream.read_to_end(&mut request).unwrap();
                println!("request: {:?}", request);
                let length = u32::from_be_bytes(request[0..4].try_into().unwrap());
                println!("length: {}", length);
                let request: &RequestHeader = &request[4..].into();
                println!("request: {:?}", request);
                let response = Response {
                    header: ResponseHeader {
                        correlation_id: request.correlation_id,
                    },
                    body: ResponseBody {
                    },
                };
                let mut buffer = BytesMut::with_capacity(1024);
                buffer.put_u32(0);
                let mut msg = buffer.split_off(4);
                msg.extend_from_slice(&response.header.correlation_id.to_be_bytes());
                buffer.copy_from_slice(&(msg.len() as u32).to_be_bytes());
                buffer.unsplit(msg);
                
                stream.write(&buffer).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[test]
fn it_works() {
    let response = Response {
        header: ResponseHeader {
            correlation_id: 7,
        },
        body: ResponseBody {
        },
    };
    let mut buffer = BytesMut::with_capacity(1024);
    buffer.put_u32(0);
    let mut msg = buffer.split_off(4);
    msg.extend_from_slice(&response.header.correlation_id.to_be_bytes());
    buffer.copy_from_slice(&(msg.len() as u32).to_be_bytes());
    buffer.unsplit(msg);
    println!("buffer: {:?}", buffer);
}
