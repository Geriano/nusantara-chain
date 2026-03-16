use borsh::{BorshDeserialize, BorshSerialize};

use crate::signed_shred::SignedShred;

pub const MAX_UDP_PACKET: usize = 65507;

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum TurbineMessage {
    Shred(SignedShred),
    RepairRequest(RepairRequest),
    RepairResponse(SignedShred),
    BatchRepairResponse(BatchRepairResponse),
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum RepairRequest {
    Shred { slot: u64, index: u32 },
    ShredBatch { slot: u64, indices: Vec<u32> },
    HighestShred { slot: u64 },
    Orphan { slot: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct BatchRepairResponse {
    pub slot: u64,
    pub shreds: Vec<SignedShred>,
}

impl BatchRepairResponse {
    /// Greedily pack shreds into UDP-safe chunks.
    /// Each chunk is wrapped in `TurbineMessage::BatchRepairResponse` and must
    /// serialize to at most `max_packet_size` bytes.
    pub fn pack(slot: u64, shreds: Vec<SignedShred>, max_packet_size: usize) -> Vec<Self> {
        if shreds.is_empty() {
            return Vec::new();
        }

        let mut batches = Vec::new();
        let mut current = Vec::new();

        // Estimate overhead: TurbineMessage enum tag (1) + slot (8) + vec length prefix (4)
        let overhead = 13;

        let mut current_size = overhead;

        for shred in shreds {
            let shred_size = borsh::to_vec(&shred).map(|b| b.len()).unwrap_or(0);

            if !current.is_empty() && current_size + shred_size > max_packet_size {
                batches.push(BatchRepairResponse {
                    slot,
                    shreds: std::mem::take(&mut current),
                });
                current_size = overhead;
            }

            current_size += shred_size;
            current.push(shred);
        }

        if !current.is_empty() {
            batches.push(BatchRepairResponse {
                slot,
                shreds: current,
            });
        }

        batches
    }
}

impl TurbineMessage {
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, String> {
        borsh::to_vec(self).map_err(|e| e.to_string())
    }

    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, String> {
        borsh::from_slice(bytes).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_crypto::Keypair;
    use nusantara_storage::shred::DataShred;
    use crate::signed_shred::SignedDataShred;

    #[test]
    fn shred_message_roundtrip() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![42u8; 100],
            flags: 0,
        };
        let signed = SignedShred::Data(SignedDataShred::new(shred, kp.address(), &kp));
        let msg = TurbineMessage::Shred(signed);

        let bytes = msg.serialize_to_bytes().unwrap();
        let decoded = TurbineMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn repair_request_roundtrip() {
        let msg = TurbineMessage::RepairRequest(RepairRequest::Shred {
            slot: 10,
            index: 5,
        });
        let bytes = msg.serialize_to_bytes().unwrap();
        let decoded = TurbineMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn shred_batch_request_roundtrip() {
        let msg = TurbineMessage::RepairRequest(RepairRequest::ShredBatch {
            slot: 42,
            indices: vec![0, 3, 7, 15],
        });
        let bytes = msg.serialize_to_bytes().unwrap();
        let decoded = TurbineMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn batch_repair_response_roundtrip() {
        let kp = Keypair::generate();
        let shreds: Vec<SignedShred> = (0..3)
            .map(|i| {
                let shred = DataShred {
                    slot: 5,
                    index: i,
                    parent_offset: 1,
                    data: vec![i as u8; 100],
                    flags: 0,
                };
                SignedShred::Data(SignedDataShred::new(shred, kp.address(), &kp))
            })
            .collect();

        let msg = TurbineMessage::BatchRepairResponse(BatchRepairResponse {
            slot: 5,
            shreds,
        });
        let bytes = msg.serialize_to_bytes().unwrap();
        let decoded = TurbineMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn batch_pack_splits_on_size() {
        let kp = Keypair::generate();
        let shreds: Vec<SignedShred> = (0..10)
            .map(|i| {
                let shred = DataShred {
                    slot: 1,
                    index: i,
                    parent_offset: 1,
                    data: vec![0u8; 1000],
                    flags: 0,
                };
                SignedShred::Data(SignedDataShred::new(shred, kp.address(), &kp))
            })
            .collect();

        // Use a small max size to force multiple batches
        let batches = BatchRepairResponse::pack(1, shreds.clone(), 10_000);
        assert!(batches.len() > 1, "should split into multiple batches");

        // All shreds accounted for
        let total: usize = batches.iter().map(|b| b.shreds.len()).sum();
        assert_eq!(total, 10);

        // Each batch serializes within limit
        for batch in &batches {
            let msg = TurbineMessage::BatchRepairResponse(batch.clone());
            let bytes = msg.serialize_to_bytes().unwrap();
            assert!(bytes.len() <= 10_000);
        }
    }

    #[test]
    fn batch_pack_empty() {
        let batches = BatchRepairResponse::pack(1, Vec::new(), MAX_UDP_PACKET);
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_pack_single_shred() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![0u8; 100],
            flags: 0,
        };
        let signed = SignedShred::Data(SignedDataShred::new(shred, kp.address(), &kp));

        let batches = BatchRepairResponse::pack(1, vec![signed], MAX_UDP_PACKET);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].shreds.len(), 1);
    }
}
