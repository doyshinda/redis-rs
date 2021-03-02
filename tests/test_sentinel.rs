#![cfg(feature = "sentinel")]
mod support;
use crate::support::*;

use redis::sentinel::Sentinel;

use std::{thread, time::Duration};

#[test]
fn test_sentinel_get_master_missing_is_error() {
    let ctx = TestSentinelContext::new();
    let mut s = Sentinel::new(ctx.sentinel_addrs()).unwrap();

    assert!(s.master_for("foo").is_err());
}

#[test]
fn test_sentinel_get_master() {
    let ctx = TestSentinelContext::new();
    let mut s = Sentinel::new(ctx.sentinel_addrs()).unwrap();

    for _ in 0..3 {
        match s.master_for(TEST_SERVICE) {
            Ok(m) => {
                assert_eq!(m, format!("redis://127.0.0.1:{}/", MASTER_PORT));
                break;
            }
            _ => thread::sleep(Duration::from_millis(500)),
        }
    }
}

#[test]
fn test_sentinel_master_updated_on_failover() {
    let mut ctx = TestSentinelContext::new();
    let mut s = Sentinel::new(ctx.sentinel_addrs()).unwrap();

    for _ in 0..3 {
        match s.master_for(TEST_SERVICE) {
            Ok(m) => {
                assert_eq!(m, format!("redis://127.0.0.1:{}/", MASTER_PORT));
                break;
            }
            _ => thread::sleep(Duration::from_millis(500)),
        }
    }

    thread::sleep(Duration::from_millis(1000));

    ctx.master.stop();
    let expected_new_master = format!("redis://127.0.0.1:{}/", REPLICA_PORT);
    let mut got = None;
    for _ in 0..10 {
        match s.master_for(TEST_SERVICE) {
            Ok(m) => {
                if m != expected_new_master {
                    thread::sleep(Duration::from_millis(1000));
                } else {
                    got = Some(m);
                    break;
                }
            }
            _ => thread::sleep(Duration::from_millis(1000)),
        }
    }

    assert_eq!(got.unwrap(), expected_new_master);
}
