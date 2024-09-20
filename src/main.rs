#![allow(unused_imports)]
use std::io::{Read, Write};

use anyhow::{Error, Result};
use api_version::{ApiKey, ApiVersion};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fetch::FetchResponse;
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
        let buffer = response_to_bytes(&response);
        println!("buffer: {:?}", buffer.to_vec());

        stream.write(&buffer).await.unwrap();
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
            api_keys: (3, vec![
                ApiKey {
                    api_key: request.header.request_api_key,
                    min_version: 0,
                    max_version: 4,
                },
                ApiKey {
                    api_key: 1,
                    min_version: 0,
                    max_version: 16,
                },
            ]),
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
    buffer[..].into()
}
