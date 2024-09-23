use bytes::BufMut;

use crate::{api_version::ApiVersion, fetch::FetchResponse};

#[derive(Debug)]
pub struct Response {
    pub(crate) header: ResponseHeader,
    pub(crate) body: ResponseBody,
}

impl Into<Vec<u8>> for &Response {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&Into::<Vec<u8>>::into(&self.header)[..]);
        buffer.extend_from_slice(&Into::<Vec<u8>>::into(&self.body)[..]);
        buffer
    }
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub(crate) correlation_id: i32,
}

impl Into<Vec<u8>> for &ResponseHeader {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersion(ApiVersion),
    Fetch(FetchResponse),
}

impl Into<Vec<u8>> for &ResponseBody {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match self {
            ResponseBody::ApiVersion(api_version) => {
                buffer.extend_from_slice(&Into::<Vec<u8>>::into(api_version)[..]);
            }
            ResponseBody::Fetch(fetch_response) => {
                buffer.extend_from_slice(&Into::<Vec<u8>>::into(fetch_response)[..]);
            }
        }
        buffer
    }
}