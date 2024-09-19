#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

use bytes::{BufMut, BytesMut};

struct Request {
}

struct Response {
    header: Header,
    body: Body,
}

struct Header {
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
                        correlation_id: 7,
                    },
                    body: Body {
                    },
                };
                let mut buffer = BytesMut::with_capacity(1024);
                let mut length = buffer.split();
                buffer.copy_from_slice(&response.header.correlation_id.to_be_bytes());
                length.put_u32(buffer.len() as u32);
                buffer.unsplit(length);
                
                stream.write(&buffer).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
