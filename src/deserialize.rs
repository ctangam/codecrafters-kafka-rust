use bytes::{Buf, Bytes};

pub trait Deserialize<T: Buf> {
    fn from_bytes(buffer: &mut T) -> Self;
}