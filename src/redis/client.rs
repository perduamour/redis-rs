extern mod extra;

use std::io::net::ip::SocketAddr;
use std::io::net::get_host_addresses;
use std::io::io_error;
use std::io::net::tcp::TcpStream;
use std::from_str::from_str;

use extra::url::Url;

use enums::*;
use connection::Connection;

mod macros;

pub struct Client {
    priv addr: SocketAddr,
    priv db: i64,
}

impl Client {

    /// creates a client.  The client will immediately connect but it will
    /// close the connection again until get_connection() is called.  The name
    /// resolution currently only happens initially.
    pub fn open(uri: &str) -> Result<Client, ConnectFailure> {
        let parsed_uri = try_unwrap!(from_str::<Url>(uri), Err(InvalidURI));
        ensure!(parsed_uri.scheme == ~"redis", Err(InvalidURI));

        let ip_addrs = try_unwrap!(get_host_addresses(parsed_uri.host), Err(InvalidURI));
        let ip_addr = try_unwrap!(ip_addrs.iter().next(), Err(HostNotFound));
        let port = try_unwrap!(from_str::<u16>(parsed_uri.port.clone()
            .unwrap_or(~"6379")), Err(InvalidURI));
        let db = from_str::<i64>(parsed_uri.path.trim_chars(&'/')).unwrap_or(0);

        let addr = SocketAddr {
            ip: *ip_addr,
            port: port
        };

        // pretty ugly way to figure out if we failed to connect.  Is there a
        // nicer way?
        let mut failed = false;
        io_error::cond.trap(|_e| {
            failed = true;
        }).inside(|| {
            let _ = try_unwrap!(TcpStream::connect(addr), failed = true);
        });
        if failed {
            return Err(ConnectionRefused);
        }

        Ok(Client {
            addr: addr,
            db: db,
        })
    }

    /// returns an independent connection for this client.  This currently
    /// does not put it into a pool.
    pub fn get_connection(&self) -> Result<Connection, ConnectFailure> {
        Connection::new(self.addr, self.db)
    }
}