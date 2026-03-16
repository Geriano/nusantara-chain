use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use nusantara_core::native_token::const_parse_u64;

pub const MAX_TX_PER_SECOND_PER_IP: u64 =
    const_parse_u64(env!("NUSA_TPU_MAX_TX_PER_SECOND_PER_IP"));
pub const MAX_TX_PER_SECOND_GLOBAL: u64 =
    const_parse_u64(env!("NUSA_TPU_MAX_TX_PER_SECOND_GLOBAL"));
pub const MAX_CONNECTIONS_PER_IP: u64 =
    const_parse_u64(env!("NUSA_TPU_MAX_CONNECTIONS_PER_IP"));

struct IpState {
    tx_count: u64,
    connection_count: u64,
    window_start: Instant,
}

pub struct RateLimiter {
    ip_states: DashMap<IpAddr, IpState>,
    global_count: AtomicU64,
    global_window_start: parking_lot::Mutex<Instant>,
    max_tx_per_sec_per_ip: u64,
    max_tx_per_sec_global: u64,
    max_connections_per_ip: u64,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            ip_states: DashMap::new(),
            global_count: AtomicU64::new(0),
            global_window_start: parking_lot::Mutex::new(Instant::now()),
            max_tx_per_sec_per_ip: MAX_TX_PER_SECOND_PER_IP,
            max_connections_per_ip: MAX_CONNECTIONS_PER_IP,
            max_tx_per_sec_global: MAX_TX_PER_SECOND_GLOBAL,
        }
    }

    /// Check if a transaction from the given IP is allowed.
    pub fn check_rate_limit(&self, ip: IpAddr) -> Result<(), crate::error::TpuError> {
        // Check global rate
        self.check_global_rate()?;

        // Check per-IP rate
        let mut entry = self.ip_states.entry(ip).or_insert_with(|| IpState {
            tx_count: 0,
            connection_count: 0,
            window_start: Instant::now(),
        });

        // Reset window if > 1 second elapsed
        if entry.window_start.elapsed().as_secs() >= 1 {
            entry.tx_count = 0;
            entry.window_start = Instant::now();
        }

        if entry.tx_count >= self.max_tx_per_sec_per_ip {
            metrics::counter!("tpu_rate_limited_per_ip_total").increment(1);
            return Err(crate::error::TpuError::RateLimited {
                reason: format!("per-IP limit exceeded: {ip}"),
            });
        }

        entry.tx_count += 1;
        self.global_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn check_global_rate(&self) -> Result<(), crate::error::TpuError> {
        let mut window_start = self.global_window_start.lock();
        if window_start.elapsed().as_secs() >= 1 {
            self.global_count.store(0, Ordering::Relaxed);
            *window_start = Instant::now();
        }

        if self.global_count.load(Ordering::Relaxed) >= self.max_tx_per_sec_global {
            metrics::counter!("tpu_rate_limited_global_total").increment(1);
            return Err(crate::error::TpuError::RateLimited {
                reason: "global rate limit exceeded".to_string(),
            });
        }

        Ok(())
    }

    /// Check if a new connection from the given IP is allowed.
    pub fn check_connection_limit(&self, ip: IpAddr) -> Result<(), crate::error::TpuError> {
        let entry = self.ip_states.entry(ip).or_insert_with(|| IpState {
            tx_count: 0,
            connection_count: 0,
            window_start: Instant::now(),
        });

        if entry.connection_count >= self.max_connections_per_ip {
            return Err(crate::error::TpuError::RateLimited {
                reason: format!("connection limit exceeded: {ip}"),
            });
        }

        Ok(())
    }

    /// Track a new connection from an IP.
    pub fn add_connection(&self, ip: IpAddr) {
        self.ip_states
            .entry(ip)
            .or_insert_with(|| IpState {
                tx_count: 0,
                connection_count: 0,
                window_start: Instant::now(),
            })
            .connection_count += 1;
    }

    /// Track a connection close from an IP.
    pub fn remove_connection(&self, ip: IpAddr) {
        if let Some(mut entry) = self.ip_states.get_mut(&ip) {
            entry.connection_count = entry.connection_count.saturating_sub(1);
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn config_values() {
        assert_eq!(MAX_TX_PER_SECOND_PER_IP, 100);
        assert_eq!(MAX_TX_PER_SECOND_GLOBAL, 50000);
        assert_eq!(MAX_CONNECTIONS_PER_IP, 8);
    }

    #[test]
    fn allows_within_limit() {
        let limiter = RateLimiter::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        for _ in 0..50 {
            assert!(limiter.check_rate_limit(ip).is_ok());
        }
    }

    #[test]
    fn rejects_over_per_ip_limit() {
        let limiter = RateLimiter::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        for _ in 0..MAX_TX_PER_SECOND_PER_IP {
            assert!(limiter.check_rate_limit(ip).is_ok());
        }
        assert!(limiter.check_rate_limit(ip).is_err());
    }

    #[test]
    fn different_ips_independent() {
        let limiter = RateLimiter::new();
        let ip1 = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let ip2 = IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8));

        for _ in 0..MAX_TX_PER_SECOND_PER_IP {
            limiter.check_rate_limit(ip1).unwrap();
        }
        // ip1 is exhausted, but ip2 should still work
        assert!(limiter.check_rate_limit(ip2).is_ok());
    }

    #[test]
    fn connection_tracking() {
        let limiter = RateLimiter::new();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);

        for _ in 0..MAX_CONNECTIONS_PER_IP {
            limiter.add_connection(ip);
        }
        assert!(limiter.check_connection_limit(ip).is_err());

        limiter.remove_connection(ip);
        assert!(limiter.check_connection_limit(ip).is_ok());
    }
}
