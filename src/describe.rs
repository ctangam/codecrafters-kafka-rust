use bytes::{Buf, BufMut};

use crate::deserialize::Deserialize;

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequest {
    topics: (u8, Vec<(u8, String)>),
    response_partition_limit: i32,
    cursor: Cursor,
}

impl<T: Buf> Deserialize<T> for DescribeTopicPartitionsRequest {
    fn from_bytes(buffer: &mut T) -> Self {
        let mut topics = (buffer.get_u8(), Vec::new());
        println!("topics: {}", topics.0);
        for _ in 0..(topics.0 - 1) {
            let len = buffer.get_u8();
            println!("topic_name: {}", len);
            let topic_name = (
                len,
                String::from_utf8_lossy(&buffer.copy_to_bytes(len as usize - 1)).to_string(),
            );
            println!("{:?}", topic_name);
            topics.1.push(topic_name);
        }
        buffer.get_u8();
        let response_partition_limit = buffer.get_i32();
        let cursor = Cursor::from_bytes(buffer);
        buffer.get_u8();

        Self {
            topics,
            response_partition_limit,
            cursor,
        }
    }
}

#[derive(Debug, Clone)]
struct Cursor {
    pub topic_name: (u8, String),
    pub partition_index: i32,
}

impl<T: Buf> Deserialize<T> for Cursor {
    fn from_bytes(buffer: &mut T) -> Self {
        let len = buffer.get_u8();
        println!("topic_name: {}", len);
        let topic_name = (
            len,
            String::from_utf8_lossy(&buffer.copy_to_bytes(len as usize - 1)).to_string(),
        );
        let partition_index = buffer.get_i32();
        buffer.get_u8();

        Self {
            topic_name,
            partition_index,
        }
    }
}

impl Into<Vec<u8>> for &Cursor {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.topic_name.0.to_be_bytes());
        buffer.extend_from_slice(&self.topic_name.1.as_bytes());
        buffer.extend_from_slice(&self.partition_index.to_be_bytes());
        buffer
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponse {
    throttle_time_ms: i32,
    topics: (u8, Vec<Topic>),
    next_cursor: Cursor,
}

impl DescribeTopicPartitionsResponse {
    pub fn new(error_code: i16, request: &DescribeTopicPartitionsRequest) -> Self {
        let topics = (
            request.topics.0,
            request
                .topics
                .1
                .iter()
                .map(|topic| Topic::new(error_code, topic.clone()))
                .collect(),
        );
        Self {
            throttle_time_ms: 0,
            topics,
            next_cursor: request.cursor.clone(),
        }
    }
}

impl Into<Vec<u8>> for &DescribeTopicPartitionsResponse {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.extend_from_slice(&self.topics.0.to_be_bytes());
        self.topics
            .1
            .iter()
            .for_each(|topic| buffer.extend_from_slice(&Into::<Vec<u8>>::into(topic)));
        buffer.extend_from_slice(&Into::<Vec<u8>>::into(&self.next_cursor));
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct Topic {
    error_code: i16,
    name: (u8, String),
    topic_id: u128,
    is_internal: bool,
    partitions: (u8, Vec<Partition>),
    topic_authorized_operations: i32,
}

impl Topic {
    fn new(error_code: i16, name: (u8, String)) -> Self {
        Self {
            error_code,
            name,
            topic_id: 0,
            is_internal: false,
            partitions: (0, Vec::new()),
            topic_authorized_operations: 0,
        }
    }
}

impl Into<Vec<u8>> for &Topic {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.name.0.to_be_bytes());
        buffer.extend_from_slice(&self.name.1.as_bytes());
        buffer.extend_from_slice(&self.topic_id.to_be_bytes());
        buffer.put_u8(self.is_internal as u8);
        buffer.extend_from_slice(&self.partitions.0.to_be_bytes());
        self.partitions
            .1
            .iter()
            .for_each(|partition| buffer.extend_from_slice(&Into::<Vec<u8>>::into(partition)));
        buffer.extend_from_slice(&self.topic_authorized_operations.to_be_bytes());
        buffer.put_u8(0);
        buffer
    }
}

#[derive(Debug)]
struct Partition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: (u8, Vec<i32>),
    isr_nodes: (u8, Vec<i32>),
    eligible_leader_replicas: (u8, Vec<i32>),
    last_known_elr: (u8, Vec<i32>),
    offline_replicas: (u8, Vec<i32>),
}

impl Into<Vec<u8>> for &Partition {
    fn into(self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&self.partition_index.to_be_bytes());
        buffer.extend_from_slice(&self.leader_id.to_be_bytes());
        buffer.extend_from_slice(&self.leader_epoch.to_be_bytes());
        buffer.extend_from_slice(&self.replica_nodes.0.to_be_bytes());
        self.replica_nodes.1.iter().for_each(|node| {
            buffer.extend_from_slice(&node.to_be_bytes());
        });
        buffer.extend_from_slice(&self.isr_nodes.0.to_be_bytes());
        self.isr_nodes.1.iter().for_each(|node| {
            buffer.extend_from_slice(&node.to_be_bytes());
        });
        buffer.extend_from_slice(&self.eligible_leader_replicas.0.to_be_bytes());
        self.eligible_leader_replicas.1.iter().for_each(|node| {
            buffer.extend_from_slice(&node.to_be_bytes());
        });
        buffer.extend_from_slice(&self.last_known_elr.0.to_be_bytes());
        self.last_known_elr.1.iter().for_each(|node| {
            buffer.extend_from_slice(&node.to_be_bytes());
        });
        buffer.extend_from_slice(&self.offline_replicas.0.to_be_bytes());
        self.offline_replicas.1.iter().for_each(|node| {
            buffer.extend_from_slice(&node.to_be_bytes());
        });
        buffer.put_u8(0);
        buffer
    }
}
