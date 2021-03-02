#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(missing_docs)]
use crate::client::Client;
use crate::cmd::Cmd;
use crate::connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo};
use crate::types::{from_redis_value, ErrorKind, RedisResult, Value};

use std::{collections::HashMap, time::Duration};

pub struct Sentinel {
    initial_sentinels: Vec<ConnectionInfo>,
    connections: HashMap<String, Connection>,
}

const SENTINEL_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const MASTER_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

impl Sentinel {
    pub fn new<T: IntoConnectionInfo>(sentinels: Vec<T>) -> RedisResult<Sentinel> {
        let mut initial_sentinels = Vec::new();
        let connections = HashMap::new();

        for sentinel in sentinels {
            initial_sentinels.push(sentinel.into_connection_info()?);
        }

        let s = Sentinel {
            initial_sentinels,
            connections,
        };
        Ok(s)
    }

    pub fn master_for(&mut self, service_name: &str) -> RedisResult<String> {
        self.connect_all_sentinels()?;
        let mut cmd = Cmd::new();
        cmd.arg("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(service_name);

        for (_, conn) in self.connections.iter_mut() {
            let raw_val = match cmd.query(conn) {
                Ok(v) => v,
                Err(_) => continue,
            };

            match raw_val {
                Value::Nil => continue,
                _ => match from_redis_value::<Vec<String>>(&raw_val) {
                    Ok(resp) => {
                        let master_addr = format!("redis://{}:{}/", resp[0], resp[1]);
                        match validate_master_role(master_addr.as_str()) {
                            Ok(is_master) => {
                                if is_master {
                                    return Ok(master_addr);
                                }
                            }
                            _ => continue,
                        }
                    }
                    _ => continue,
                },
            }
        }

        fail!((
            ErrorKind::InvalidClientConfig,
            "Master for requested service name not found"
        ));
    }

    fn connect_all_sentinels(&mut self) -> RedisResult<()> {
        for info in &self.initial_sentinels {
            let addr = info.addr.to_string();
            match self
                .connections
                .get_mut(&addr)
                .and_then(|c| Some(c.check_connection()))
            {
                Some(true) => continue,
                _ => match connect(info, Some(SENTINEL_CONNECT_TIMEOUT)) {
                    Ok(conn) => {
                        self.connections.insert(addr, conn);
                    }
                    Err(e) => {
                        println!("Error connecting to {}: {:?}", addr, e);
                        self.connections.remove(&addr);
                    }
                },
            }
        }

        if self.connections.is_empty() {
            fail!((ErrorKind::IoError, "Unable to reach any sentinels"));
        }

        Ok(())
    }
}

fn validate_master_role<T: IntoConnectionInfo>(addr: T) -> RedisResult<bool> {
    let client = Client::open(addr)?;
    let mut con = client.get_connection_with_timeout(MASTER_CONNECT_TIMEOUT)?;
    let mut cmd = Cmd::new();
    let result: Vec<String> = cmd.arg("ROLE").query(&mut con)?;
    return Ok((result.len() > 0) && (&result[0] == "master"));
}
