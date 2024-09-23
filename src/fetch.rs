use bytes::{Buf, BufMut};

/*
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
*/
#[derive(Debug)]
pub struct FetchRequest {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: (i8, Vec<Topic>),
    forgotten_topics_data: (i8, Vec<ForgottenTopicsData>),
    rack_id: (i8, String),
}

impl From<&[u8]> for FetchRequest {
    fn from(mut buffer: &[u8]) -> Self {
        let max_wait_ms = buffer.get_i32();
        let min_bytes = buffer.get_i32();
        let max_bytes = buffer.get_i32();
        let isolation_level = buffer.get_i8();
        let session_id = buffer.get_i32();
        let session_epoch = buffer.get_i32();
        let mut topics = (buffer.get_i8(), Vec::new());
        for _ in 0..topics.0 {
            let topic = Topic::from(buffer);
            topics.1.push(topic);
        }

        let mut forgotten_topics_data = (buffer.get_i8(), Vec::new());
        for _ in 0..forgotten_topics_data.0 {
            let data = ForgottenTopicsData::from(buffer);
            forgotten_topics_data.1.push(data);
        }

        let mut rack_id = (buffer.get_i8(), String::from(""));

        rack_id.1 = String::from_utf8(buffer.take(rack_id.0 as usize).into_inner().to_vec()).unwrap();
        
        buffer.get_u8();

        Self {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        }
    }
}

#[derive(Debug)]
pub struct Topic {
    topic_id: u128,
    partitions: (i8, Vec<PartitionReq>),
}

impl From<&[u8]> for Topic {
    fn from(mut buffer: &[u8]) -> Self {
        let topic_id = buffer.get_u128();
        let mut partitions = (buffer.get_i8(), Vec::new());

        for _ in 0..partitions.0 {
            let partition = PartitionReq::from(buffer);
            partitions.1.push(partition);
        }
        buffer.get_u8();

        Self {
            topic_id,
            partitions,
        }
    }
}

#[derive(Debug)]
pub struct PartitionReq {
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

impl From<&[u8]> for PartitionReq {
    fn from(mut buffer: &[u8]) -> Self {
        let partition = buffer.get_i32();
        let current_leader_epoch = buffer.get_i32();
        let fetch_offset = buffer.get_i64();
        let last_fetched_epoch = buffer.get_i32();
        let log_start_offset = buffer.get_i64();
        let partition_max_bytes = buffer.get_i32();

        buffer.get_u8();

        Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
        }

    }
}

#[derive(Debug)]
pub struct ForgottenTopicsData {
    topic_id: u128,
    partitions: (i8, Vec<i32>),
}

impl From<&[u8]> for ForgottenTopicsData {
    fn from(mut buffer: &[u8]) -> Self {
        let topic_id = buffer.get_u128();
        let mut partitions = (buffer.get_i8(), Vec::new());

        for _ in 0..partitions.0 {
            let partition = buffer.get_i32();
            partitions.1.push(partition);
        }
        buffer.get_u8();

        Self {
            topic_id,
            partitions,
        }
    }
}

/*
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
*/
#[derive(Debug)]
pub struct FetchResponse {
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    responses: (i8, Vec<Response>),
}

impl FetchResponse {
    pub fn new(error_code: i16, session_id: i32) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            session_id,
            responses: (0, Vec::new()),
        }
    }
}

impl From<&[u8]> for FetchResponse {
    fn from(mut buffer: &[u8]) -> Self {
        let throttle_time_ms = buffer.get_i32();
        let error_code = buffer.get_i16();
        let session_id = buffer.get_i32();

        let mut responses = (buffer.get_i8(), Vec::new());

        for _ in 0..responses.0 {
            let response = Response::from(buffer);
            responses.1.push(response);
        }

        buffer.get_u8();

        Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        }
    }
}

impl Into<Vec<u8>> for &FetchResponse {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.session_id.to_be_bytes());
        buffer.put_i8(self.responses.0);
        buffer.extend_from_slice(
            &self
                .responses
                .1
                .iter()
                .map(|response| Into::<Vec<u8>>::into(response))
                .collect::<Vec<Vec<u8>>>()
                .concat(),
        );
        buffer
    }
}

#[derive(Debug)]
struct Response {
    topic_id: u128,
    partitions: (i8, Vec<PartitionResp>),
}

impl From<&[u8]> for Response {
    fn from(mut buffer: &[u8]) -> Self {
        let topic_id = buffer.get_u128();
        let mut partitions = (buffer.get_i8(), Vec::new());

        for _ in 0..partitions.0 {
            let partition = PartitionResp::from(buffer);
            partitions.1.push(partition);
        }
        buffer.get_u8();

        Self {
            topic_id,
            partitions,
        }
    }
}

impl Into<Vec<u8>> for &Response {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.topic_id.to_be_bytes());
        buffer.extend_from_slice(
            &self
                .partitions
                .1
                .iter()
                .map(|partition| Into::<Vec<u8>>::into(partition))
                .collect::<Vec<Vec<u8>>>()
                .concat(),
        );
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct PartitionResp {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: (i8, Vec<AbortedTransaction>),
    preferred_read_replica: i32,
    records: (i8, Vec<u8>),
}

impl From<&[u8]> for PartitionResp {
    fn from(mut buffer: &[u8]) -> Self {
        let partition_index = buffer.get_i32();
        let error_code = buffer.get_i16();
        let high_watermark = buffer.get_i64();
        let last_stable_offset = buffer.get_i64();
        let log_start_offset = buffer.get_i64();
        let mut aborted_transactions = (buffer.get_i8(), Vec::new());
        for _ in 0..aborted_transactions.0 {
            let aborted_transaction = AbortedTransaction::from(buffer);
            aborted_transactions.1.push(aborted_transaction);
        }
        let preferred_read_replica = buffer.get_i32();
        let mut records = (buffer.get_i8(), Vec::new());
        for _ in 0..records.0 {
            let record = buffer.get_u8();
            records.1.push(record);
        }
        buffer.get_u8();
        
        Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
        }
    }
}

impl Into<Vec<u8>> for &PartitionResp {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.partition_index.to_be_bytes());
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.high_watermark.to_be_bytes());
        buffer.extend_from_slice(&self.last_stable_offset.to_be_bytes());
        buffer.extend_from_slice(&self.log_start_offset.to_be_bytes());
        buffer.extend_from_slice(
            &self
                .aborted_transactions
                .1
                .iter()
                .map(|aborted_transaction| Into::<Vec<u8>>::into(aborted_transaction))
                .collect::<Vec<Vec<u8>>>()
                .concat(),
        );
        buffer.extend_from_slice(&self.preferred_read_replica.to_be_bytes());
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct AbortedTransaction {
    producer_id: i64,
    first_offset: i64,
}

impl From<&[u8]> for AbortedTransaction {
    fn from(mut buffer: &[u8]) -> Self {
        let producer_id = buffer.get_i64();
        let first_offset = buffer.get_i64();
        buffer.get_u8();

        Self {
            producer_id,
            first_offset,
        }
    }
}

impl Into<Vec<u8>> for &AbortedTransaction {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.producer_id.to_be_bytes());
        buffer.extend_from_slice(&self.first_offset.to_be_bytes());
        buffer.put_u8(0);
        buffer
    }
}