use std::io::{Read, Write};

use bytes::Buf;

use crate::fetch::FetchRequest;

#[derive(Debug)]
pub struct Request {
    pub(crate) header: RequestHeader,
    pub(crate) body: RequestBody,
}

impl From<&[u8]> for Request {
    fn from(mut buffer: &[u8]) -> Self {
        let header = RequestHeader::from(buffer);
        match header.request_api_key {
            1 => {
                let body = RequestBody::Fetch(FetchRequest::from(buffer));
                Self { header, body }
            }
            18 => {
                let body = RequestBody::ApiVersion;
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
    client_id: String,
    _tagged_fields: Option<Vec<i32>>,
}

impl From<&[u8]> for RequestHeader {
    fn from(mut buffer: &[u8]) -> Self {
        let request_api_key = buffer.get_i16();
        let request_api_version = buffer.get_i16();
        let correlation_id = buffer.get_i32();
        buffer.get_i16();
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
pub enum RequestBody {
    ApiVersion,
    Fetch(FetchRequest),
}