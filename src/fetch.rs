use std::borrow::BorrowMut;

use bytes::{Buf, BufMut, Bytes};

use crate::deserialize::Deserialize;

/*
Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
*/
#[derive(Debug)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: (u8, Vec<Topic>),
    pub forgotten_topics_data: (u8, Vec<ForgottenTopicsData>),
    pub rack_id: (u8, String),
}

impl<T: Buf> Deserialize<T> for FetchRequest {
    fn from_bytes(buffer: &mut T) -> Self {
        let max_wait_ms = buffer.get_i32();
        let min_bytes = buffer.get_i32();
        let max_bytes = buffer.get_i32();
        let isolation_level = buffer.get_i8();
        let session_id = buffer.get_i32();
        let session_epoch = buffer.get_i32();
        let mut topics = (buffer.get_u8(), Vec::new());
        for _ in 0..topics.0-1 {
            let topic = Topic::from_bytes(buffer);
            println!("{:?}", topic);
            topics.1.push(topic);
        }

        let mut forgotten_topics_data = (buffer.get_u8(), Vec::new());
        println!("forgotten_topics_data: {}", forgotten_topics_data.0);
        for _ in 0..forgotten_topics_data.0-1 {
            let data = ForgottenTopicsData::from_bytes(buffer);
            forgotten_topics_data.1.push(data);
        }

        let mut rack_id = (buffer.get_u8(), String::new());
        println!("rack_id: {}", rack_id.0);

        rack_id.1 = String::from_utf8_lossy(&buffer.copy_to_bytes(rack_id.0 as usize - 1)).to_string();
        
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
    partitions: (u8, Vec<PartitionReq>),
}

impl<T: Buf> Deserialize<T> for Topic {
    fn from_bytes(buffer: &mut T) -> Self {
        let topic_id = buffer.get_u128();
        println!("topic_id: {}", topic_id);
        let mut partitions = (buffer.get_u8(), Vec::new());
        println!("partitions: {}", partitions.0);
        for _ in 0..partitions.0-1 {
            let partition = PartitionReq::from_bytes(buffer);
            println!("{:?}", partition);
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

impl<T: Buf> Deserialize<T> for PartitionReq {
    fn from_bytes(buffer: &mut T) -> Self {
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
    partitions: (u8, Vec<i32>),
}

impl<T: Buf> Deserialize<T> for ForgottenTopicsData {
    fn from_bytes(mut buffer: &mut T) -> Self {
        let topic_id = buffer.get_u128();
        let mut partitions = (buffer.get_u8(), Vec::new());

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
    responses: (u8, Vec<Response>),
}

impl FetchResponse {
    pub fn new(error_code: i16, request: &FetchRequest) -> Self {
        let responses = request.topics.1.iter().map(|topic| Response::new(topic.topic_id)).collect();
        Self {
            throttle_time_ms: 0,
            error_code,
            session_id: request.session_id,
            responses: (request.topics.0, responses),
        }
    }
}

impl Into<Vec<u8>> for &FetchResponse {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.session_id.to_be_bytes());
        buffer.put_u8(self.responses.0);
        buffer.extend_from_slice(
            &self
                .responses
                .1
                .iter()
                .map(|response| Into::<Vec<u8>>::into(response))
                .collect::<Vec<Vec<u8>>>()
                .concat(),
        );
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct Response {
    topic_id: u128,
    partitions: (u8, Vec<PartitionResp>),
}

impl Response {
    fn new(topic_id: u128) -> Self {
        Self {
            topic_id,
            partitions: (2, vec![PartitionResp::new(0, 100)]),
        }
    }
}

impl Into<Vec<u8>> for &Response {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.topic_id.to_be_bytes());
        buffer.put_u8(self.partitions.0);
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
    aborted_transactions: (u8, Vec<AbortedTransaction>),
    preferred_read_replica: i32,
    records: (u8, Vec<u8>),
}

impl PartitionResp {
    fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: (0, vec![]),
            preferred_read_replica: 0,
            records: (0, vec![]),
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
        buffer.put_u8(self.aborted_transactions.0);
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
        buffer.put_u8(self.records.0);
        buffer.extend_from_slice(&self.records.1);
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct AbortedTransaction {
    producer_id: i64,
    first_offset: i64,
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