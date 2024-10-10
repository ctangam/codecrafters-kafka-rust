use std::io::{Read, Write};

use bytes::{Buf, Bytes};

use crate::{describe::DescribeTopicPartitionsRequest, deserialize::Deserialize, fetch::FetchRequest};

#[derive(Debug)]
pub struct Request {
    pub(crate) header: RequestHeader,
    pub(crate) body: RequestBody,
}

impl<T: Buf> Deserialize<T> for Request {
    fn from_bytes(buffer: &mut T) -> Self {
        let header = RequestHeader::from_bytes(buffer);
        match header.request_api_key {
            1 => {
                let body = RequestBody::Fetch(FetchRequest::from_bytes(buffer));
                Self { header, body }
            }
            18 => {
                let body = RequestBody::ApiVersion;
                Self { header, body }
            }
            75 => {
                let body = RequestBody::Describe(DescribeTopicPartitionsRequest::from_bytes(buffer));
                Self { header, body }
            }
            _ => todo!(),
        }
    }
}



#[derive(Debug)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    client_id: (i16, String),
    _tagged_fields: Option<Vec<i32>>,
}

impl<T: Buf> Deserialize<T> for RequestHeader {
    fn from_bytes(buffer: &mut T) -> Self {
        let request_api_key = buffer.get_i16();
        let request_api_version = buffer.get_i16();
        let correlation_id = buffer.get_i32();
        let mut client_id = (buffer.get_i16(), String::new());
        client_id.1 = String::from_utf8_lossy(&buffer.copy_to_bytes(client_id.0 as usize)).to_string();
        buffer.get_u8();

        RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _tagged_fields: None,
        }
    }
}

#[derive(Debug)]
pub enum RequestBody {
    ApiVersion,
    Fetch(FetchRequest),
    Describe(DescribeTopicPartitionsRequest),
}