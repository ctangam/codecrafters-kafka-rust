#![allow(unused_imports)]
use std::{io::{Read, Write}};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

#[derive(Debug)]
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

#[derive(Debug)]
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
    length: i8,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
}

impl Into<Vec<u8>> for &ApiVersion {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.length.to_be_bytes());
        buffer.extend_from_slice(&self.api_keys.iter().map(|api_key| Into::<Vec<u8>>::into(api_key)).collect::<Vec<Vec<u8>>>().concat());
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.put_u8(0);
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
        buffer.put_u8(0);
        buffer
    }
}


#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                loop {
                    let request = read_request(&mut stream).await;
                    println!("request: {:?}", &request);
                    let response = build_response(&request);
                    let buffer = response_to_bytes(&response);
                    println!("buffer: {:?}", buffer.to_vec());
                    
                    stream.write(&buffer).await.unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn response_to_bytes(response: &Response) -> BytesMut {
    let mut buffer = BytesMut::new();
    buffer.put_u32(0);
    let mut msg = buffer.split_off(4);
    msg.extend_from_slice(&Into::<Vec<u8>>::into(response)[..]);
    buffer.copy_from_slice(&(msg.len() as u32).to_be_bytes());
    buffer.unsplit(msg);
    buffer
}

fn build_response(request: &Request) -> Response {
    let error_code = match request.header.request_api_version {
        0..=4 => 0,
        _ => 35,
    };
    let response = Response {
        header: ResponseHeader {
            correlation_id: request.header.correlation_id,
        },
        body: ResponseBody::ApiVersion(ApiVersion {
            error_code,
            length: 2,
            api_keys: vec![ApiKey {
                api_key: request.header.request_api_key,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: 0,
        }),
    };
    response
}

async fn read_request(stream: &mut TcpStream) -> Request {
    let mut buffer = [0; 4];
    stream.read_exact(&mut buffer).await.unwrap();
    let length = u32::from_be_bytes(buffer);
    let mut buffer = vec![0; length as usize];
    stream.read_exact(&mut buffer).await.unwrap();
    let header: RequestHeader = buffer[..].into();
    let body = RequestBody {};
    Request {
        header,
        body,
    }
}