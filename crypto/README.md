# nusantara-crypto

Post-quantum cryptographic library for the Nusantara blockchain.

## Features

- **SHA3-512** hashing (64-byte output) for maximum collision resistance
- **Dilithium3 (ML-DSA-65)** post-quantum digital signatures (NIST FIPS 204)
- **NEAR-like account IDs** with `.nusantara` suffix
- **Base64 URL-safe** encoding (no padding) for all user-facing data
- **Merkle trees** with domain separation and proof generation/verification
- **bitcode** serialization via serde compatibility (`bitcode::serde`)

## Size Reference

| Type | Bytes | Base64url Chars |
|------|-------|-----------------|
| Hash | 64 | 86 |
| PublicKey | 1,952 | 2,603 |
| SecretKey | 4,032 | (never displayed) |
| Signature | 3,309 | 4,412 |

## Usage

### Hashing

```rust
use nusantara_crypto::{hash, hashv, Hasher};

let h = hash(b"hello nusantara");
println!("{}", h); // base64url encoded

let h2 = hashv(&[b"hello", b" nusantara"]);
assert_eq!(h, h2);

let mut hasher = Hasher::new();
hasher.update(b"hello");
hasher.update(b" nusantara");
assert_eq!(h, hasher.finalize());
```

### Keypair Generation, Signing, and Verification

```rust
use nusantara_crypto::Keypair;

let keypair = Keypair::generate();
let message = b"transfer 100 tokens";
let signature = keypair.sign(message);

signature.verify(keypair.public_key(), message)
    .expect("verification failed");
```

### Account IDs

```rust
use nusantara_crypto::{AccountId, Keypair};

// Named accounts (NEAR-like)
let alice = AccountId::named("alice.nusantara").unwrap();
let dex = AccountId::named("dex.alice.nusantara").unwrap();
assert!(dex.is_sub_account_of("alice.nusantara"));

// Implicit accounts (derived from public key)
let keypair = Keypair::generate();
let implicit = keypair.public_key().to_account_id();
assert!(implicit.is_implicit());
```

### Merkle Trees

```rust
use nusantara_crypto::{hash, MerkleTree};

let leaves: Vec<_> = (0..8).map(|i| hash(&[i])).collect();
let tree = MerkleTree::new(&leaves);
let proof = tree.proof(3).unwrap();
assert!(proof.verify(&leaves[3], &tree.root()));
```

### Serialization

```rust
use nusantara_crypto::hash;

let h = hash(b"data");

// JSON (human-readable) - uses base64url strings
let json = serde_json::to_string(&h).unwrap();

// bitcode (binary) - via serde compatibility
let bytes = bitcode::serialize(&h).unwrap();
let decoded: nusantara_crypto::Hash = bitcode::deserialize(&bytes).unwrap();
```

## Encoding

All user-facing data uses **Base64 URL-safe encoding without padding** (RFC 4648 section 5).
This means no `+`, `/`, or `=` characters appear in encoded output.

## Post-Quantum Signatures

This crate uses Dilithium3 (ML-DSA-65) from the NIST FIPS 204 standard via the
`pqcrypto` crate family. Dilithium3 offers the best balance of security level,
signature size, and verification speed among NIST post-quantum signature candidates.

## Build Requirements

This crate depends on `pqcrypto-dilithium` which uses C FFI bindings (PQClean).
A C compiler (`cc`) is required at build time.
