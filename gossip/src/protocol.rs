use borsh::{BorshDeserialize, BorshSerialize};
use nusantara_crypto::{Hash, Signature};

use crate::bloom::BloomFilter;
use crate::crds_value::CrdsValue;

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum GossipMessage {
    PullRequest(PullRequest),
    PullResponse(PullResponse),
    PushMessage(PushMessage),
    PruneMessage(PruneMessage),
    Ping(PingMessage),
    Pong(PongMessage),
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PullRequest {
    pub filter: BloomFilter,
    pub self_value: CrdsValue,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PullResponse {
    pub from: Hash,
    pub values: Vec<CrdsValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PushMessage {
    pub from: Hash,
    pub values: Vec<CrdsValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PruneMessage {
    pub from: Hash,
    pub prunes: Vec<Hash>,
    pub destination: Hash,
    pub wallclock: u64,
    pub signature: Signature,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PingMessage {
    pub from: Hash,
    pub token: Hash,
    pub signature: Signature,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct PongMessage {
    pub from: Hash,
    pub token_hash: Hash,
    pub signature: Signature,
}

impl GossipMessage {
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
    use nusantara_crypto::{Keypair, hash};

    #[test]
    fn ping_pong_roundtrip() {
        let kp = Keypair::generate();
        let token = hash(b"ping_token");
        let sig = kp.sign(token.as_bytes());

        let ping = GossipMessage::Ping(PingMessage {
            from: kp.address(),
            token,
            signature: sig,
        });

        let bytes = ping.serialize_to_bytes().unwrap();
        let decoded = GossipMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(ping, decoded);
    }

    #[test]
    fn push_message_roundtrip() {
        let kp = Keypair::generate();
        let ci = crate::contact_info::ContactInfo::new(
            kp.public_key().clone(),
            "127.0.0.1:8000".parse().unwrap(),
            "127.0.0.1:8003".parse().unwrap(),
            "127.0.0.1:8004".parse().unwrap(),
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:8002".parse().unwrap(),
            1,
            1000,
        );
        let value = CrdsValue::new_signed(
            crate::crds_value::CrdsData::ContactInfo(ci),
            &kp,
        );
        let msg = GossipMessage::PushMessage(PushMessage {
            from: kp.address(),
            values: vec![value],
        });

        let bytes = msg.serialize_to_bytes().unwrap();
        let decoded = GossipMessage::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }
}
