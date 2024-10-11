use std::io::{Cursor, Seek, SeekFrom};

use bytes::Buf;

use crate::deserialize::Deserialize;

#[derive(Debug)]
pub struct ClusterMetadata(Vec<RecordBatch>);

impl<T: Buf> Deserialize<T> for ClusterMetadata {
    fn from_bytes(buffer: &mut T) -> Self {
        let mut record_batches = Vec::new();
        while buffer.has_remaining() {
            let batch = RecordBatch::from_bytes(buffer);
            record_batches.push(batch);
        }

        Self(record_batches)
    }
}

#[derive(Debug)]
struct RecordBatch {
    base_offset: u64,
    batch_length: u32,
    partition_leader_epoch: u32,
    magic_byte: u8,
    crc: u32,
    attributes: u16,
    last_offset_delta: u32,
    base_timestamp: u64,
    max_timestamp: u64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_length: u32,
    records: Vec<Record>,
}

impl<T: Buf> Deserialize<T> for RecordBatch {
    fn from_bytes(buffer: &mut T) -> Self {
        let base_offset = buffer.get_u64();
        let batch_length = buffer.get_u32();
        let partition_leader_epoch = buffer.get_u32();
        let magic_byte = buffer.get_u8();
        let crc = buffer.get_u32();
        let attributes = buffer.get_u16();
        let last_offset_delta = buffer.get_u32();
        let base_timestamp = buffer.get_u64();
        let max_timestamp = buffer.get_u64();
        let producer_id = buffer.get_i64();
        let producer_epoch = buffer.get_i16();
        let base_sequence = buffer.get_i32();
        let records_length = buffer.get_u32();
        let mut records = Vec::new();
        for _ in 0..records_length - 1 {
            let record = Record::from_bytes(buffer);
            records.push(record);
        }

        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic_byte,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_length,
            records,
        }
    }
}

#[derive(Debug)]
struct Record {
    length: u8,
    attributes: u8,
    timestamp_delta: u8,
    offset_delta: u8,
    key_length: i8,
    key: Option<u8>,
    value_length: i8,
    value: Value,
    headers_array_count: u8,
}

impl<T: Buf> Deserialize<T> for Record {
    fn from_bytes(buffer: &mut T) -> Self {
        let length = buffer.get_u8();
        let attributes = buffer.get_u8();
        let timestamp_delta = buffer.get_u8();
        let offset_delta = buffer.get_u8();
        let key_length = buffer.get_i8();
        let key = if key_length != 1 { Some(buffer.get_u8()) } else { None };
        let value_length = buffer.get_i8();
        let value = Value::from_bytes(buffer);
        let headers_array_count = buffer.get_u8();

        Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value_length,
            value,
            headers_array_count,
        }
    }
}

#[derive(Debug)]
enum Value {
    FeatureLevelRecord(FeatureLevelRecord),
    TopicRecord(TopicRecord),
    PartitionRecord(PartitionRecord),
}

impl<T: Buf> Deserialize<T> for Value {
    fn from_bytes(buffer: &mut T) -> Self {
        let mut buf = Cursor::new(buffer);
        let pos = buf.position();
        buf.get_mut().advance(1);
        let r#type = buf.get_mut().get_u8();
        buf.set_position(pos);
        
        match r#type {
            0 => Self::FeatureLevelRecord(FeatureLevelRecord::from_bytes(buf.get_mut())),
            1 => Self::TopicRecord(TopicRecord::from_bytes(buf.get_mut())),
            2 => Self::PartitionRecord(PartitionRecord::from_bytes(buf.get_mut())),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
struct FeatureLevelRecord {
    frame_version: u8,
    r#type: u8,
    version: u8,
    name_length: i8,
    name: String,
    feature_level: u16,
    tagged_fields_count: u8,
}

impl<T: Buf> Deserialize<T> for FeatureLevelRecord {
    fn from_bytes(buffer: &mut T) -> Self {
        let frame_version = buffer.get_u8();
        let r#type = buffer.get_u8();
        let version = buffer.get_u8();
        let name_length = buffer.get_i8();
        let name = String::from_utf8_lossy(&buffer.copy_to_bytes(name_length as usize - 1)).to_string();
        let feature_level = buffer.get_u16();
        let tagged_fields_count = buffer.get_u8();

        Self {
            frame_version,
            r#type,
            version,
            name_length,
            name,
            feature_level,
            tagged_fields_count,
        }
    }
}

#[derive(Debug)]
struct TopicRecord {
    frame_version: u8,
    r#type: u8,
    version: u8,
    name_length: i8,
    topic_name: String,
    topic_uuid: u128,
    tagged_fields_count: u8,
}

impl<T: Buf> Deserialize<T> for TopicRecord {
    fn from_bytes(buffer: &mut T) -> Self {
        let frame_version = buffer.get_u8();
        let r#type = buffer.get_u8();
        let version = buffer.get_u8();
        let name_length = buffer.get_i8();
        let topic_name = String::from_utf8_lossy(&buffer.copy_to_bytes(name_length as usize - 1)).to_string();
        let topic_uuid = buffer.get_u128();
        let tagged_fields_count = buffer.get_u8();

        Self {
            frame_version,
            r#type,
            version,
            name_length,
            topic_name,
            topic_uuid,
            tagged_fields_count,
        }
    }
}

#[derive(Debug)]
struct PartitionRecord {
    frame_version: u8,
    r#type: u8,
    version: u8,
    partition_id: u32,
    topic_uuid: u128,
    length_of_replica_array: u8,
    replica_array: Vec<u32>,
    length_of_isr_array: u8,
    isr_array: Vec<u32>,
    length_of_rr_array: u8,
    rr_array: Vec<u32>,
    length_of_ar_array: u8,
    ar_array: Vec<u32>,
    leader: u32,
    leader_epoch: u32,
    partiton_epoch: u32,
    length_of_directories_array: u8,
    directories_array: Vec<u128>,
    tagged_fields_count: u8,
}

impl<T: Buf> Deserialize<T> for PartitionRecord {
    fn from_bytes(buffer: &mut T) -> Self {
        let frame_version = buffer.get_u8();
        let r#type = buffer.get_u8();
        let version = buffer.get_u8();
        let partition_id = buffer.get_u32();
        let topic_uuid = buffer.get_u128();
        let length_of_replica_array = buffer.get_u8();
        let mut replica_array = Vec::new();
        for _ in 0..length_of_replica_array - 1 {
            replica_array.push(buffer.get_u32());
        }
        let length_of_isr_array = buffer.get_u8();
        let mut isr_array = Vec::new();
        for _ in 0..length_of_isr_array - 1 {
            isr_array.push(buffer.get_u32());
        }
        let length_of_rr_array = buffer.get_u8();
        let mut rr_array = Vec::new();
        for _ in 0..length_of_rr_array - 1 {
            rr_array.push(buffer.get_u32());
        }
        let length_of_ar_array = buffer.get_u8();
        let mut ar_array = Vec::new();
        for _ in 0..length_of_ar_array - 1 {
            ar_array.push(buffer.get_u32());
        }
        let leader = buffer.get_u32();
        let leader_epoch = buffer.get_u32();
        let partiton_epoch = buffer.get_u32();
        let length_of_directories_array = buffer.get_u8();
        let mut directories_array = Vec::new();
        for _ in 0..length_of_directories_array - 1 {
            directories_array.push(buffer.get_u128());
        }
        let tagged_fields_count = buffer.get_u8();

        Self {
            frame_version,
            r#type,
            version,
            partition_id,
            topic_uuid,
            length_of_replica_array,
            replica_array,
            length_of_isr_array,
            isr_array,
            length_of_rr_array,
            rr_array,
            length_of_ar_array,
            ar_array,
            leader,
            leader_epoch,
            partiton_epoch,
            length_of_directories_array,
            directories_array,
            tagged_fields_count,
        }
    }
}