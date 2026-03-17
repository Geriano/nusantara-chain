use borsh::{BorshDeserialize, BorshSerialize};
use nusantara_crypto::{Hash, Keypair, PublicKey, Signature};
use nusantara_storage::shred::{CodeShred, DataShred};

/// Signed data shred with cached serialized bytes for performance.
/// The `cached_bytes` field is NOT serialized over the wire — it is
/// reconstructed on deserialization for zero-copy verification.
#[derive(Clone, Debug)]
pub struct SignedDataShred {
    pub shred: DataShred,
    pub leader: Hash,
    pub signature: Signature,
    /// Cached borsh serialization of `shred` — used for signing/verification.
    cached_bytes: Vec<u8>,
}

impl PartialEq for SignedDataShred {
    fn eq(&self, other: &Self) -> bool {
        self.shred == other.shred
            && self.leader == other.leader
            && self.signature == other.signature
    }
}

impl Eq for SignedDataShred {}

// Manual BorshSerialize: skip cached_bytes (wire-compatible)
impl BorshSerialize for SignedDataShred {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.shred, writer)?;
        BorshSerialize::serialize(&self.leader, writer)?;
        BorshSerialize::serialize(&self.signature, writer)?;
        Ok(())
    }
}

// Manual BorshDeserialize: populate cached_bytes from deserialized shred
impl BorshDeserialize for SignedDataShred {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let shred = DataShred::deserialize_reader(reader)?;
        let leader = Hash::deserialize_reader(reader)?;
        let signature = Signature::deserialize_reader(reader)?;
        let cached_bytes = borsh::to_vec(&shred)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Self {
            shred,
            leader,
            signature,
            cached_bytes,
        })
    }
}

impl SignedDataShred {
    pub fn new(shred: DataShred, leader: Hash, keypair: &Keypair) -> Self {
        let cached_bytes = borsh::to_vec(&shred).expect("shred serialization cannot fail");
        let signature = keypair.sign(&cached_bytes);
        Self {
            shred,
            leader,
            signature,
            cached_bytes,
        }
    }

    pub fn verify(&self, pubkey: &PublicKey) -> bool {
        self.signature.verify(pubkey, &self.cached_bytes).is_ok()
    }

    /// Access the cached serialized shred bytes (for FEC encoding, etc.).
    pub fn shred_bytes(&self) -> &[u8] {
        &self.cached_bytes
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

/// Signed code shred with cached serialized bytes for performance.
#[derive(Clone, Debug)]
pub struct SignedCodeShred {
    pub shred: CodeShred,
    pub leader: Hash,
    pub signature: Signature,
    cached_bytes: Vec<u8>,
}

impl PartialEq for SignedCodeShred {
    fn eq(&self, other: &Self) -> bool {
        self.shred == other.shred
            && self.leader == other.leader
            && self.signature == other.signature
    }
}

impl Eq for SignedCodeShred {}

impl BorshSerialize for SignedCodeShred {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.shred, writer)?;
        BorshSerialize::serialize(&self.leader, writer)?;
        BorshSerialize::serialize(&self.signature, writer)?;
        Ok(())
    }
}

impl BorshDeserialize for SignedCodeShred {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let shred = CodeShred::deserialize_reader(reader)?;
        let leader = Hash::deserialize_reader(reader)?;
        let signature = Signature::deserialize_reader(reader)?;
        let cached_bytes = borsh::to_vec(&shred)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Self {
            shred,
            leader,
            signature,
            cached_bytes,
        })
    }
}

impl SignedCodeShred {
    pub fn new(shred: CodeShred, leader: Hash, keypair: &Keypair) -> Self {
        let cached_bytes = borsh::to_vec(&shred).expect("shred serialization cannot fail");
        let signature = keypair.sign(&cached_bytes);
        Self {
            shred,
            leader,
            signature,
            cached_bytes,
        }
    }

    pub fn verify(&self, pubkey: &PublicKey) -> bool {
        self.signature.verify(pubkey, &self.cached_bytes).is_ok()
    }

    pub fn shred_bytes(&self) -> &[u8] {
        &self.cached_bytes
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

    #[test]
    fn cached_bytes_used_for_verification() {
        let kp = Keypair::generate();
        let shred = DataShred {
            slot: 1,
            index: 0,
            parent_offset: 1,
            data: vec![42u8; 100],
            flags: 0,
        };
        let signed = SignedDataShred::new(shred.clone(), kp.address(), &kp);

        // Verify that shred_bytes matches what borsh::to_vec would produce
        let expected = borsh::to_vec(&shred).unwrap();
        assert_eq!(signed.shred_bytes(), &expected[..]);

        // Verify after deserialization roundtrip
        let bytes = borsh::to_vec(&signed).unwrap();
        let decoded: SignedDataShred = borsh::from_slice(&bytes).unwrap();
        assert!(decoded.verify(kp.public_key()));
        assert_eq!(decoded.shred_bytes(), &expected[..]);
    }
}
