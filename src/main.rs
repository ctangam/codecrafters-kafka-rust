#![allow(unused_imports)]
use std::io::{Read, Write};

use anyhow::{Error, Result};
use api_version::{ApiKey, ApiVersion};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use describe::DescribeTopicPartitionsResponse;
use deserialize::Deserialize;
use fetch::FetchResponse;
use pretty_hex::PrettyHex;
use pretty_hex::pretty_hex;
use request::{Request, RequestBody, RequestHeader};
use response::{Response, ResponseBody, ResponseHeader};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod request;
mod response;
mod api_version;
mod fetch;
mod deserialize;
mod describe;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(stream).await;
        });
    }
}

async fn process(mut stream: TcpStream) {
    println!("accepted new connection");
    loop {
        let request = read_request(&mut stream).await;
        println!("request: {:?}", &request);
        let response = build_response(&request);
        println!("response: {:?}", &response);
        let buffer = response_to_bytes(&response);
        println!("{:?}", buffer.hex_dump());

        stream.write(&buffer).await.unwrap();
    }
}

fn response_to_bytes(response: &Response) -> BytesMut {
    let mut buffer = BytesMut::new();
    buffer.put_u32(0);
    let mut msg = buffer.split_off(4);
    msg.extend_from_slice(&Into::<Vec<u8>>::into(response)[..]);
    println!("length: {}", msg.len());
    buffer.copy_from_slice(&(msg.len() as u32).to_be_bytes());
    buffer.unsplit(msg);
    buffer
}

fn build_response(request: &Request) -> Response {

    let body = match request.body {
        RequestBody::Fetch(ref fetch) => {
            let error_code = match request.header.request_api_version {
                0..=16 => 0,
                _ => 35,
            };
            ResponseBody::Fetch(FetchResponse::new(error_code, fetch))
        },
        RequestBody::ApiVersion => {
            let error_code = match request.header.request_api_version {
                0..=4 => 0,
                _ => 35,
            };
            ResponseBody::ApiVersion(ApiVersion::new(error_code))
        },
        RequestBody::Describe(ref describe) => {
            let error_code = match request.header.request_api_version {
                0 => 0,
                _ => 3,
            };
            ResponseBody::Describe(DescribeTopicPartitionsResponse::new(3, describe))
        }
        _ => unimplemented!()
    };
    let response = Response {
        header: ResponseHeader {
            correlation_id: request.header.correlation_id,
        },
        body,
    };
    response
}

async fn read_request(stream: &mut TcpStream) -> Request {
    let mut buffer = [0; 4];
    stream.read_exact(&mut buffer).await.unwrap();
    let length = u32::from_be_bytes(buffer);
    let mut buffer = vec![0; length as usize];
    stream.read_exact(&mut buffer).await.unwrap();
    println!("{:?}", buffer.hex_dump());

    Request::from_bytes(&mut &buffer[..])
}
