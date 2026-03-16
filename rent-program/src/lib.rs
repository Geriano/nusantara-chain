use borsh::{BorshDeserialize, BorshSerialize};

use nusantara_core::native_token::{const_parse_u64, const_parse_u8};

pub const DEFAULT_LAMPORTS_PER_BYTE_YEAR: u64 =
    const_parse_u64(env!("NUSA_RENT_LAMPORTS_PER_BYTE_YEAR"));
pub const DEFAULT_EXEMPTION_THRESHOLD: u64 =
    const_parse_u64(env!("NUSA_RENT_EXEMPTION_THRESHOLD"));
pub const DEFAULT_BURN_PERCENT: u8 = const_parse_u8(env!("NUSA_RENT_BURN_PERCENT"));

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Rent {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: u64,
    pub burn_percent: u8,
}

impl Rent {
    pub fn minimum_balance(&self, data_len: usize) -> u64 {
        let bytes = 128u64 + data_len as u64; // account overhead + data
        bytes
            .saturating_mul(self.lamports_per_byte_year)
            .saturating_mul(self.exemption_threshold)
    }

    pub fn due(&self, lamports: u64, data_len: usize, years_elapsed: f64) -> RentDue {
        let min = self.minimum_balance(data_len);
        if lamports >= min {
            return RentDue::Exempt;
        }

        let bytes = 128u64 + data_len as u64;
        let rent = (bytes as f64 * self.lamports_per_byte_year as f64 * years_elapsed) as u64;
        RentDue::Paying(rent)
    }
}

impl Default for Rent {
    fn default() -> Self {
        Self {
            lamports_per_byte_year: DEFAULT_LAMPORTS_PER_BYTE_YEAR,
            exemption_threshold: DEFAULT_EXEMPTION_THRESHOLD,
            burn_percent: DEFAULT_BURN_PERCENT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RentDue {
    Exempt,
    Paying(u64),
}

#[derive(Clone, Debug)]
pub struct RentCollector {
    pub epoch: u64,
    pub slots_per_year: f64,
    pub rent: Rent,
}

impl RentCollector {
    pub fn new(epoch: u64, slots_per_year: f64, rent: Rent) -> Self {
        Self {
            epoch,
            slots_per_year,
            rent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rent_values() {
        assert_eq!(DEFAULT_LAMPORTS_PER_BYTE_YEAR, 3480);
        assert_eq!(DEFAULT_EXEMPTION_THRESHOLD, 2);
        assert_eq!(DEFAULT_BURN_PERCENT, 50);
    }

    #[test]
    fn minimum_balance() {
        let rent = Rent::default();
        // 128 (overhead) + 0 (data) = 128 bytes
        // 128 * 3480 * 2 = 890880
        assert_eq!(rent.minimum_balance(0), 890_880);
    }

    #[test]
    fn due_exempt() {
        let rent = Rent::default();
        let min = rent.minimum_balance(100);
        assert_eq!(rent.due(min, 100, 1.0), RentDue::Exempt);
        assert_eq!(rent.due(min + 1, 100, 1.0), RentDue::Exempt);
    }

    #[test]
    fn due_paying() {
        let rent = Rent::default();
        let due = rent.due(0, 100, 1.0);
        assert!(matches!(due, RentDue::Paying(_)));
    }

    #[test]
    fn borsh_roundtrip() {
        let rent = Rent::default();
        let encoded = borsh::to_vec(&rent).unwrap();
        let decoded: Rent = borsh::from_slice(&encoded).unwrap();
        assert_eq!(rent, decoded);
    }
}
