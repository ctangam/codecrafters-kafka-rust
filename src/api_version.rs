use bytes::BufMut;

#[derive(Debug)]
pub struct ApiVersion {
    pub(crate) error_code: i16,
    pub(crate) api_keys: (u8, Vec<ApiKey>),
    pub(crate) throttle_time_ms: i32,
}

impl ApiVersion {
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            api_keys: (
                3,
                vec![
                    ApiKey {
                        api_key: 18,
                        min_version: 0,
                        max_version: 4,
                    },
                    ApiKey {
                        api_key: 1,
                        min_version: 0,
                        max_version: 16,
                    },
                ],
            ),
            throttle_time_ms: 0,
        }
    }
}

impl Into<Vec<u8>> for &ApiVersion {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.api_keys.0.to_be_bytes());
        buffer.extend_from_slice(
            &self
                .api_keys
                .1
                .iter()
                .map(|api_key| Into::<Vec<u8>>::into(api_key))
                .collect::<Vec<Vec<u8>>>()
                .concat(),
        );
        buffer.put_u8(0);
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
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
