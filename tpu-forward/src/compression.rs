use crate::error::TpuError;

/// Tag byte prefixed to all wire messages.
const TAG_RAW: u8 = 0x00;
const TAG_ZSTD: u8 = 0x01;

/// Payloads at or below this size are sent raw (compression overhead not worth it).
const COMPRESSION_THRESHOLD: usize = 256;

/// Zstd compression level (3 = fast with good ratio).
const ZSTD_LEVEL: i32 = 3;

/// Maximum decompressed-to-wire ratio to guard against decompression bombs.
pub const MAX_DECOMPRESSION_RATIO: usize = 4;

/// Compress `data` with a 1-byte tag prefix.
pub fn compress(data: &[u8]) -> Result<Vec<u8>, TpuError> {
    if data.is_empty() {
        return Err(TpuError::Compression("empty payload".to_string()));
    }

    if data.len() <= COMPRESSION_THRESHOLD {
        let mut out = Vec::with_capacity(1 + data.len());
        out.push(TAG_RAW);
        out.extend_from_slice(data);
        return Ok(out);
    }

    let compressed =
        zstd::encode_all(data, ZSTD_LEVEL).map_err(|e| TpuError::Compression(e.to_string()))?;

    if compressed.len() >= data.len() {
        let mut out = Vec::with_capacity(1 + data.len());
        out.push(TAG_RAW);
        out.extend_from_slice(data);
        return Ok(out);
    }

    metrics::counter!("nusantara_wire_compressed_total").increment(1);
    let ratio = compressed.len() as f64 / data.len() as f64;
    metrics::histogram!("nusantara_wire_compression_ratio").record(ratio);

    let mut out = Vec::with_capacity(1 + compressed.len());
    out.push(TAG_ZSTD);
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress a tagged payload produced by [`compress`].
/// `max_decompressed_size` limits the output to prevent decompression bombs.
pub fn decompress(tagged: &[u8], max_decompressed_size: usize) -> Result<Vec<u8>, TpuError> {
    if tagged.is_empty() {
        return Err(TpuError::Decompression("empty payload".to_string()));
    }

    match tagged[0] {
        TAG_RAW => Ok(tagged[1..].to_vec()),
        TAG_ZSTD => {
            let decompressed = zstd::decode_all(&tagged[1..])
                .map_err(|e| TpuError::Decompression(e.to_string()))?;
            if decompressed.len() > max_decompressed_size {
                return Err(TpuError::Decompression(format!(
                    "decompressed size {} exceeds max {}",
                    decompressed.len(),
                    max_decompressed_size
                )));
            }
            Ok(decompressed)
        }
        tag => Err(TpuError::Decompression(format!(
            "unknown compression tag: 0x{tag:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let data = vec![42u8; 1024];
        let compressed = compress(&data).unwrap();
        let decompressed = decompress(&compressed, 8192).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn below_threshold_raw() {
        let data = vec![1u8; 100];
        let compressed = compress(&data).unwrap();
        assert_eq!(compressed[0], TAG_RAW);
        assert_eq!(&compressed[1..], &data[..]);
    }

    #[test]
    fn decompression_bomb_guard() {
        let data = vec![0u8; 4096];
        let compressed = compress(&data).unwrap();
        // Allow only 100 bytes — should fail
        assert!(decompress(&compressed, 100).is_err());
    }

    #[test]
    fn empty_payload_error() {
        assert!(compress(&[]).is_err());
        assert!(decompress(&[], 1024).is_err());
    }

    #[test]
    fn invalid_tag_error() {
        let bad = vec![0xFF, 1, 2, 3];
        assert!(decompress(&bad, 1024).is_err());
    }
}
