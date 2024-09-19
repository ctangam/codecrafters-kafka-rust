#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

use bytes::BytesMut;

struct Request {
    length: u32,
}

struct Response {
    header: Header,
    body: Body,
}

struct Header {
    length: u32,
    correlation_id: i32,
}

struct Body {

}


fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let response = Response {
                    header: Header {
                        length: 0,
                        correlation_id: 7,
                    },
                    body: Body {
                    },
                };
                let mut buffer = BytesMut::new();
                buffer.copy_from_slice(&response.header.length.to_be_bytes());
                buffer.copy_from_slice(&response.header.correlation_id.to_be_bytes());
        
                stream.write(&buffer).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
