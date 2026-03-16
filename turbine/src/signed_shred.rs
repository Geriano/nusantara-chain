use borsh::{BorshDeserialize, BorshSerialize};
use nusantara_crypto::{Hash, Keypair, PublicKey, Signature};
use nusantara_storage::shred::{CodeShred, DataShred};

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SignedDataShred {
    pub shred: DataShred,
    pub leader: Hash,
    pub signature: Signature,
}

impl SignedDataShred {
    pub fn new(shred: DataShred, leader: Hash, keypair: &Keypair) -> Self {
        let data = borsh::to_vec(&shred).expect("shred serialization cannot fail");
        let signature = keypair.sign(&data);
        Self {
            shred,
            leader,
            signature,
        }
    }

    pub fn verify(&self, pubkey: &PublicKey) -> bool {
        let data = match borsh::to_vec(&self.shred) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };
        self.signature.verify(pubkey, &data).is_ok()
    }

    pub fn slot(&self) -> u64 {
        self.shred.slot
    }

    pub fn index(&self) -> u32 {
        self.shred.index
    }

    pub fn is_last(&self) -> bool {
        self.shred.flags & 0x01 != 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SignedCodeShred {
    pub shred: CodeShred,
    pub leader: Hash,
    pub signature: Signature,
}

impl SignedCodeShred {
    pub fn new(shred: CodeShred, leader: Hash, keypair: &Keypair) -> Self {
        let data = borsh::to_vec(&shred).expect("shred serialization cannot fail");
        let signature = keypair.sign(&data);
        Self {
            shred,
            leader,
            signature,
        }
    }

    pub fn verify(&self, pubkey: &PublicKey) -> bool {
        let data = match borsh::to_vec(&self.shred) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };
        self.signature.verify(pubkey, &data).is_ok()
    }

    pub fn slot(&self) -> u64 {
        self.shred.slot
    }

    pub fn index(&self) -> u32 {
        self.shred.index
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum SignedShred {
    Data(SignedDataShred),
    Code(SignedCodeShred),
}

impl SignedShred {
    pub fn slot(&self) -> u64 {
        match self {
            Self::Data(s) => s.slot(),
            Self::Code(s) => s.slot(),
        }
    }

    pub fn index(&self) -> u32 {
        match self {
            Self::Data(s) => s.index(),
            Self::Code(s) => s.index(),
        }
    }

    pub fn leader(&self) -> Hash {
        match self {
            Self::Data(s) => s.leader,
            Self::Code(s) => s.leader,
        }
    }

    pub fn verify(&self, pubkey: &PublicKey) -> bool {
        match self {
            Self::Data(s) => s.verify(pubkey),
            Self::Code(s) => s.verify(pubkey),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_data_shred() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![42u8; 100],
            flags: 0,
        };
        let signed = SignedDataShred::new(shred, kp.address(), &kp);
        assert!(signed.verify(kp.public_key()));
        assert_eq!(signed.slot(), 1);
        assert_eq!(signed.index(), 0);
        assert!(!signed.is_last());
    }

    #[test]
    fn last_shred_flag() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 5,
            parent_offset: 1,
            data: vec![0u8; 50],
            flags: 0x01,
        };
        let signed = SignedDataShred::new(shred, kp.address(), &kp);
        assert!(signed.is_last());
    }

    #[test]
    fn wrong_key_fails_verification() {
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![1u8; 100],
            flags: 0,
        };
        let signed = SignedDataShred::new(shred, kp1.address(), &kp1);
        assert!(!signed.verify(kp2.public_key()));
    }

    #[test]
    fn sign_and_verify_code_shred() {
        let kp = Keypair::generate();
        let shred = CodeShred {
            slot: 1,
            index: 0,
            num_data_shreds: 10,
            num_code_shreds: 4,
            position: 0,
            data: vec![0xAB; 100],
        };
        let signed = SignedCodeShred::new(shred, kp.address(), &kp);
        assert!(signed.verify(kp.public_key()));
    }

    #[test]
    fn signed_shred_enum_borsh_roundtrip() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 5,
            index: 3,
            parent_offset: 1,
            data: vec![99u8; 50],
            flags: 0x01,
        };
        let signed = SignedShred::Data(SignedDataShred::new(shred, kp.address(), &kp));
        let bytes = borsh::to_vec(&signed).unwrap();
        let decoded: SignedShred = borsh::from_slice(&bytes).unwrap();
        assert_eq!(signed, decoded);
    }
}
