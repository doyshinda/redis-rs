#![cfg(feature = "sentinel")]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::convert::identity;
use std::fs;
use std::io::prelude::*;
use std::path::PathBuf;
use std::process;
use std::thread::sleep;
use std::time::Duration;

use super::RedisServer;

pub const MASTER_PORT: u16 = 6998;
pub const REPLICA_PORT: u16 = 6999;
pub const TEST_SERVICE: &str = "sentinel_test_service";

pub struct TestSentinelContext {
    pub master: RedisServer,
    pub replica: RedisServer,
    pub sentinels: Vec<RedisServer>,
    pub folders: Vec<PathBuf>,
}

impl TestSentinelContext {
    pub fn new() -> TestSentinelContext {
        let mut sentinels = vec![];
        let mut folders = vec![];

        let master = RedisServer::new_with_addr(
            redis::ConnectionAddr::Tcp("127.0.0.1".into(), MASTER_PORT),
            |cmd| {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = PathBuf::from(format!("/tmp/redis-rs-sentinel-test-{}-{}-dir", a, b));
                fs::create_dir_all(&path).unwrap();
                cmd.current_dir(&path);
                folders.push(path);
                dbg!(&cmd);
                cmd.spawn().unwrap()
            },
        );

        let replica = RedisServer::new_with_addr(
            redis::ConnectionAddr::Tcp("127.0.0.1".into(), REPLICA_PORT),
            |cmd| {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = PathBuf::from(format!("/tmp/redis-rs-sentinel-test-{}-{}-dir", a, b));
                fs::create_dir_all(&path).unwrap();
                cmd.current_dir(&path);
                folders.push(path);
                cmd.arg("--replicaof")
                    .arg("127.0.0.1")
                    .arg(MASTER_PORT.to_string());
                dbg!(&cmd);
                cmd.spawn().unwrap()
            },
        );

        let start_port = 5000;
        for i in 0..3 {
            let port = start_port + i;
            sentinels.push(RedisServer::new_with_addr(
                redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
                |_cmd| {
                    let (a, b) = rand::random::<(u64, u64)>();
                    let path =
                        PathBuf::from(format!("/tmp/redis-rs-sentinel-test-{}-{}-dir", a, b));
                    fs::create_dir_all(&path).unwrap();
                    let conf_file = make_sentinel_conf(MASTER_PORT, port, TEST_SERVICE, &path);
                    let mut cmd = process::Command::new("redis-server");
                    cmd.stdout(process::Stdio::null())
                        .stderr(process::Stdio::null())
                        .arg(&conf_file)
                        .arg("--sentinel")
                        .current_dir(&path);
                    folders.push(path);
                    dbg!(&cmd);
                    cmd.spawn().unwrap()
                },
            ));
        }

        let ctx = TestSentinelContext {
            master,
            replica,
            sentinels,
            folders,
        };
        ctx.wait_for_sentinels();
        ctx
    }

    pub fn sentinel_addrs(&self) -> Vec<String> {
        let mut sentinels = vec![];
        for s in &self.sentinels {
            sentinels.push(format!("redis://{}/", s.get_client_addr().to_string()));
        }
        sentinels
    }

    fn wait_for_sentinels(&self) {
        'server: for addr in self.sentinel_addrs() {
            let client = redis::Client::open(addr).unwrap();

            for _ in 1..50 {
                let mut con = match client.get_connection() {
                    Ok(c) => c,
                    _ => {
                        sleep(Duration::from_millis(100));
                        continue;
                    }
                };
                match redis::cmd("PING").query::<String>(&mut con) {
                    Ok(_) => continue 'server,
                    _ => sleep(Duration::from_millis(100)),
                }
            }

            panic!("failed to wait for sentinels to come up");
        }
    }
}

impl Drop for TestSentinelContext {
    fn drop(&mut self) {
        self.master.stop();
        self.replica.stop();
        for server in &mut self.sentinels {
            server.stop();
        }
        for folder in &self.folders {
            fs::remove_dir_all(&folder).unwrap();
        }
    }
}

fn make_sentinel_conf(
    server_port: u16,
    sentinel_port: u16,
    service_name: &str,
    path: &PathBuf,
) -> PathBuf {
    let conf_file = path.join(format!("{}.conf", sentinel_port));
    let mut file = fs::File::create(&conf_file).unwrap();
    file.write_all(format!("port {}\n", sentinel_port).as_bytes())
        .unwrap();
    file.write_all(
        format!(
            "sentinel monitor {} 127.0.0.1 {} 2\n",
            service_name, server_port
        )
        .as_bytes(),
    )
    .unwrap();
    file.write_all(format!("sentinel down-after-milliseconds {} 500\n", service_name).as_bytes())
        .unwrap();
    file.write_all(format!("sentinel failover-timeout {} 60000\n", service_name).as_bytes())
        .unwrap();
    file.write_all(format!("sentinel parallel-syncs {} 1\n", service_name).as_bytes())
        .unwrap();

    conf_file
}
