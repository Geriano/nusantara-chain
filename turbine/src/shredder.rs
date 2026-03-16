use nusantara_core::block::Block;
use nusantara_core::native_token::const_parse_u64;
use nusantara_crypto::Keypair;
use nusantara_storage::shred::{CodeShred, DataShred};

use crate::erasure::ErasureCoder;
use crate::error::TurbineError;
use crate::signed_shred::{SignedCodeShred, SignedDataShred};

pub const MAX_DATA_PER_SHRED: u64 = const_parse_u64(env!("NUSA_TURBINE_MAX_DATA_PER_SHRED"));
pub const FEC_RATE_PERCENT: u64 = const_parse_u64(env!("NUSA_TURBINE_FEC_RATE_PERCENT"));

pub struct ShredBatch {
    pub data_shreds: Vec<SignedDataShred>,
    pub code_shreds: Vec<SignedCodeShred>,
}

pub struct Shredder;

impl Shredder {
    /// Shred a block into signed data shreds + FEC code shreds.
    pub fn shred_block(
        block: &Block,
        parent_slot: u64,
        keypair: &Keypair,
    ) -> Result<ShredBatch, TurbineError> {
        let slot = block.header.slot;
        let leader = keypair.address();
        let block_bytes = borsh::to_vec(block)
            .map_err(|e| TurbineError::BlockSerialization(e.to_string()))?;

        let chunk_size = MAX_DATA_PER_SHRED as usize;
        let chunks: Vec<&[u8]> = block_bytes.chunks(chunk_size).collect();
        let num_chunks = chunks.len();

        // Create data shreds
        let mut data_shreds = Vec::with_capacity(num_chunks);
        for (i, chunk) in chunks.iter().enumerate() {
            let is_last = i == num_chunks - 1;
            let shred = DataShred {
                slot,
                index: i as u32,
                parent_offset: (slot - parent_slot) as u16,
                data: chunk.to_vec(),
                flags: if is_last { 0x01 } else { 0x00 },
            };
            data_shreds.push(SignedDataShred::new(shred, leader, keypair));
        }

        // FEC encode in groups of 32 data shreds
        let fec_group_size = 32usize;
        let mut code_shreds = Vec::new();
        let mut code_index = 0u32;

        for group_start in (0..data_shreds.len()).step_by(fec_group_size) {
            let group_end = (group_start + fec_group_size).min(data_shreds.len());
            let group = &data_shreds[group_start..group_end];
            let num_data = group.len();

            if num_data < 2 {
                // Too few shreds for FEC
                continue;
            }

            let ec = ErasureCoder::from_fec_rate(num_data, FEC_RATE_PERCENT as u32);

            // Serialize each data shred to bytes and pad to uniform length
            let shard_bytes: Vec<Vec<u8>> = group
                .iter()
                .map(|s| borsh::to_vec(&s.shred).unwrap_or_default())
                .collect();

            let max_len = shard_bytes.iter().map(|b| b.len()).max().unwrap_or(0);
            let padded: Vec<Vec<u8>> = shard_bytes
                .iter()
                .map(|b| {
                    let mut padded = b.clone();
                    padded.resize(max_len, 0);
                    padded
                })
                .collect();

            match ec.encode(&padded) {
                Ok(parity_shards) => {
                    for (j, parity) in parity_shards.iter().enumerate() {
                        let code = CodeShred {
                            slot,
                            index: code_index,
                            num_data_shreds: num_data as u32,
                            num_code_shreds: parity_shards.len() as u32,
                            position: j as u32,
                            data: parity.clone(),
                        };
                        code_shreds.push(SignedCodeShred::new(code, leader, keypair));
                        code_index += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "FEC encoding failed, skipping parity for group");
                }
            }
        }

        metrics::counter!("turbine_shreds_created_total")
            .increment((data_shreds.len() + code_shreds.len()) as u64);

        Ok(ShredBatch {
            data_shreds,
            code_shreds,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nusantara_core::block::{Block, BlockHeader};
    use nusantara_crypto::{Hash, hash};

    fn test_block(slot: u64, tx_count: usize) -> Block {
        let txs = vec![]; // empty transactions for size testing
        Block {
            header: BlockHeader {
                slot,
                parent_slot: slot.saturating_sub(1),
                parent_hash: hash(b"parent"),
                block_hash: hash(b"block"),
                timestamp: 1000,
                validator: hash(b"validator"),
                transaction_count: tx_count as u64,
                merkle_root: Hash::zero(),
                poh_hash: Hash::zero(),
                bank_hash: Hash::zero(),
                state_root: Hash::zero(),
            },
            transactions: txs,
        }
    }

    #[test]
    fn config_values() {
        assert_eq!(MAX_DATA_PER_SHRED, 1228);
        assert_eq!(FEC_RATE_PERCENT, 33);
    }

    #[test]
    fn shred_small_block() {
        let kp = Keypair::generate();
        let block = test_block(1, 0);
        let batch = Shredder::shred_block(&block, 0, &kp).unwrap();

        assert!(!batch.data_shreds.is_empty());
        // Last data shred should have the last flag
        let last = batch.data_shreds.last().unwrap();
        assert!(last.is_last());

        // All data shreds should be verifiable
        for shred in &batch.data_shreds {
            assert!(shred.verify(kp.public_key()));
            assert_eq!(shred.slot(), 1);
        }
    }

    #[test]
    fn shred_indices_sequential() {
        let kp = Keypair::generate();
        let block = test_block(5, 0);
        let batch = Shredder::shred_block(&block, 4, &kp).unwrap();

        for (i, shred) in batch.data_shreds.iter().enumerate() {
            assert_eq!(shred.index(), i as u32);
        }
    }
}
