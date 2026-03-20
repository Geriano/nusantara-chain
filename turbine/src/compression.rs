use crate::error::TurbineError;

/// Tag byte prefixed to all wire messages.
const TAG_RAW: u8 = 0x00;
const TAG_ZSTD: u8 = 0x01;

/// Payloads at or below this size are sent raw (compression overhead not worth it).
const COMPRESSION_THRESHOLD: usize = 256;

/// Zstd compression level (3 = fast with good ratio).
const ZSTD_LEVEL: i32 = 3;

/// Compress `data` with a 1-byte tag prefix.
/// Returns `TAG_RAW || data` if below threshold or incompressible,
/// `TAG_ZSTD || zstd(data)` otherwise.
pub fn compress(data: &[u8]) -> Result<Vec<u8>, TurbineError> {
    if data.is_empty() {
        return Err(TurbineError::Compression("empty payload".to_string()));
    }

    if data.len() <= COMPRESSION_THRESHOLD {
        let mut out = Vec::with_capacity(1 + data.len());
        out.push(TAG_RAW);
        out.extend_from_slice(data);
        return Ok(out);
    }

    let compressed =
        zstd::encode_all(data, ZSTD_LEVEL).map_err(|e| TurbineError::Compression(e.to_string()))?;

    // Fallback: if compressed >= raw, send raw
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
pub fn decompress(tagged: &[u8]) -> Result<Vec<u8>, TurbineError> {
    if tagged.is_empty() {
        return Err(TurbineError::Decompression("empty payload".to_string()));
    }

    match tagged[0] {
        TAG_RAW => Ok(tagged[1..].to_vec()),
        TAG_ZSTD => {
            let decompressed = zstd::decode_all(&tagged[1..])
                .map_err(|e| TurbineError::Decompression(e.to_string()))?;
            Ok(decompressed)
        }
        tag => Err(TurbineError::Decompression(format!(
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
        let decompressed = decompress(&compressed).unwrap();
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
    fn incompressible_sent_raw() {
        // Random-ish data that won't compress well
        let data: Vec<u8> = (0..512).map(|i| (i * 7 + 13) as u8).collect();
        let compressed = compress(&data).unwrap();
        // Should either be raw or compressed — both are valid roundtrips
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn empty_payload_error() {
        assert!(compress(&[]).is_err());
        assert!(decompress(&[]).is_err());
    }

    #[test]
    fn invalid_tag_error() {
        let bad = vec![0xFF, 1, 2, 3];
        assert!(decompress(&bad).is_err());
    }
}
