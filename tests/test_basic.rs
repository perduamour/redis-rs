extern crate redis;
extern crate rand; extern crate net2;

#[macro_use]
extern crate quickcheck;
extern crate partial_io;
extern crate futures;
extern crate tokio_core;

use redis::{Commands, PipelineCommands};

use std::thread::{spawn, sleep};
use std::time::Duration;
use std::collections::{HashMap, HashSet};
use std::collections::{BTreeSet,BTreeMap};
use std::io::BufReader;

use support::*;

mod support;

#[test]
fn test_parse_redis_url() {
    let redis_url = format!("redis://127.0.0.1:1234/0");
    match redis::parse_redis_url(&redis_url) {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    }
    match redis::parse_redis_url("unix:/var/run/redis/redis.sock") {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    }
    match redis::parse_redis_url("127.0.0.1") {
        Ok(_) => assert!(false),
        Err(_) => assert!(true),
    }
}

#[test]
fn test_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("key1").arg(b"foo").execute(&con);
    redis::cmd("SET").arg(&["key2", "bar"]).execute(&con);

    assert_eq!(redis::cmd("MGET").arg(&["key1", "key2"]).query(&con),
               Ok(("foo".to_string(), b"bar".to_vec())));
}

#[test]
fn test_getset() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&con);
    assert_eq!(redis::cmd("GET").arg("foo").query(&con), Ok(42));

    redis::cmd("SET").arg("bar").arg("foo").execute(&con);
    assert_eq!(redis::cmd("GET").arg("bar").query(&con),
               Ok(b"foo".to_vec()));
}

#[test]
fn test_incr() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&con);
    assert_eq!(redis::cmd("INCR").arg("foo").query(&con), Ok(43usize));
}

#[test]
fn test_info() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let info: redis::InfoDict = redis::cmd("INFO").query(&con).unwrap();
    assert_eq!(info.find(&"role"),
               Some(&redis::Value::Status("master".to_string())));
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(info.len() > 0);
    assert!(info.contains_key(&"role"));
}

#[test]
fn test_hash_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("HSET").arg("foo").arg("key_1").arg(1).execute(&con);
    redis::cmd("HSET").arg("foo").arg("key_2").arg(2).execute(&con);

    let h: HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));

    let h: BTreeMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

#[test]
fn test_set_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1).execute(&con);
    redis::cmd("SADD").arg("foo").arg(2).execute(&con);
    redis::cmd("SADD").arg("foo").arg(3).execute(&con);

    let mut s: Vec<i32> = redis::cmd("SMEMBERS").arg("foo").query(&con).unwrap();
    s.sort();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set: HashSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(&con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));

    let set: BTreeSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(&con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1).execute(&con);
    redis::cmd("SADD").arg("foo").arg(2).execute(&con);
    redis::cmd("SADD").arg("foo").arg(3).execute(&con);

    let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN").arg("foo").arg(0).query(&con).unwrap();
    s.sort();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

#[test]
fn test_optionals() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(1).execute(&con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo")
        .arg("missing")
        .query(&con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET").arg("missing").query(&con).unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

#[cfg(feature="with-rustc-json")]
#[test]
fn test_json() {
    use redis::Json;

    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg("[1, 2, 3]").execute(&con);

    let json: Json = redis::cmd("GET").arg("foo").query(&con).unwrap();
    assert_eq!(json,
               Json::Array(vec![Json::U64(1), Json::U64(2), Json::U64(3)]));
}

#[test]
fn test_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..1000 {
        redis::cmd("SADD").arg("foo").arg(x).execute(&con);
        unseen.insert(x);
    }

    let iter = redis::cmd("SSCAN").arg("foo").cursor_arg(0).iter(&con).unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_filtered_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..3000 {
        let _: () = con.hset("foo", format!("key_{}_{}", x % 100, x), x).unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let iter = con.hscan_match("foo", "key_0_*").unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["key_1", "key_2"])
        .query(&con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_empty_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let _: () = redis::pipe()
        .cmd("PING")
        .ignore()
        .query(&con)
        .unwrap();

    let _: () = redis::pipe().query(&con).unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["key_1", "key_2"])
        .query(&con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_real_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&con).unwrap();

    loop {
        let _: () = redis::cmd("WATCH").arg(key).query(&con).unwrap();
        let val: isize = redis::cmd("GET").arg(key).query(&con).unwrap();
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(&con)
            .unwrap();

        match response {
            None => {
                continue;
            }
            Some(response) => {
                assert_eq!(response, (43,));
                break;
            }
        }
    }
}

#[test]
fn test_real_transaction_highlevel() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&con).unwrap();

    let response: (isize,) = redis::transaction(&con, &[key], |pipe| {
            let val: isize = try!(redis::cmd("GET").arg(key).query(&con));
            pipe.cmd("SET")
                .arg(key)
                .arg(val + 1)
                .ignore()
                .cmd("GET")
                .arg(key)
                .query(&con)
        })
        .unwrap();

    assert_eq!(response, (43,));
}

#[test]
fn test_pubsub() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let mut pubsub = ctx.pubsub();
    pubsub.subscribe("foo").unwrap();

    let thread = spawn(move || {
        sleep(Duration::from_millis(100));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(42));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(23));
    });

    redis::cmd("PUBLISH").arg("foo").arg(42).execute(&con);
    // We can also call the command directly
    assert_eq!(con.publish("foo", 23), Ok(1));

    thread.join().ok().expect("Something went wrong");
}

#[test]
fn test_script() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let script = redis::Script::new(r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ");

    let _: () = redis::cmd("SET").arg("my_key").arg("foo").query(&con).unwrap();
    let response = script.key("my_key").arg(42).invoke(&con);

    assert_eq!(response, Ok(("foo".to_string(), 42)));
}

#[test]
fn test_tuple_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("HMSET")
        .arg("my_key")
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(&con);

    assert_eq!(redis::cmd("HGET").arg("my_key").arg("field_1").query(&con),
               Ok(42));
    assert_eq!(redis::cmd("HGET").arg("my_key").arg("field_2").query(&con),
               Ok(23));
}

#[test]
fn test_nice_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.set("my_key", 42), Ok(()));
    assert_eq!(con.get("my_key"), Ok(42));

    let (k1, k2): (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42)
        .ignore()
        .set("key_2", 43)
        .ignore()
        .get("key_1")
        .get("key_2")
        .query(&con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_auto_m_versions() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.set_multiple(&[("key1", 1), ("key2", 2)]), Ok(()));
    assert_eq!(con.get(&["key1", "key2"]), Ok((1, 2)));
}

#[test]
fn test_nice_hash_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)]),
               Ok(()));

    let hm: HashMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let hm: BTreeMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v: Vec<(String, isize)> = con.hgetall("my_hash").unwrap();
    assert_eq!(v,
               vec![("f1".to_string(), 1),
                    ("f2".to_string(), 2),
                    ("f3".to_string(), 4),
                    ("f4".to_string(), 8)]);

    assert_eq!(con.hget("my_hash", &["f2", "f4"]), Ok((2, 8)));
    assert_eq!(con.hincr("my_hash", "f1", 1), Ok((2)));
    assert_eq!(con.hincr("my_hash", "f2", 1.5f32), Ok((3.5f32)));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(true));
    assert_eq!(con.hdel("my_hash", &["f1", "f2"]), Ok(()));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(false));

    let iter: redis::Iter<(String, isize)> = con.hscan("my_hash").unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert_eq!(found.contains(&("f3".to_string(), 4)), true);
    assert_eq!(found.contains(&("f4".to_string(), 8)), true);
}

#[test]
fn test_nice_list_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.rpush("my_list", &[1, 2, 3, 4]), Ok(4));
    assert_eq!(con.rpush("my_list", &[5, 6, 7, 8]), Ok(8));
    assert_eq!(con.llen("my_list"), Ok(8));

    assert_eq!(con.lpop("my_list"), Ok(1));
    assert_eq!(con.llen("my_list"), Ok(7));

    assert_eq!(con.lrange("my_list", 0, 2), Ok((2, 3, 4)));

    assert_eq!(con.lset("my_list", 0, 4), Ok(true));
    assert_eq!(con.lrange("my_list", 0, 2), Ok((4, 3, 4)));
}

#[test]
fn test_tuple_decoding_regression() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.del("my_zset"), Ok(()));
    assert_eq!(con.zadd("my_zset", "one", 1), Ok(1));
    assert_eq!(con.zadd("my_zset", "two", 2), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(con.del("my_zset"), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 0);
}

#[test]
fn test_bit_operations() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.setbit("bitvec", 10, true), Ok(false));
    assert_eq!(con.getbit("bitvec", 10), Ok(true));
}

#[test]
fn test_invalid_protocol() {
    use std::thread;
    use std::error::Error;
    use std::io::Write;
    use std::net::TcpListener;
    use redis::{RedisResult, Parser};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let child = thread::spawn(move || -> Result<(), Box<Error + Send + Sync>> {
        let mut stream = BufReader::new(try!(listener.incoming().next().unwrap()));
        // read the request and respond with garbage
        let _: redis::Value = try!(Parser::new(&mut stream).parse_value());
        try!(stream.get_mut().write_all(b"garbage ---!#!#\r\n\r\n\n\r"));
        // block until the stream is shutdown by the client
        let _: RedisResult<redis::Value> = Parser::new(&mut stream).parse_value();
        Ok(())
    });
    sleep(Duration::from_millis(100));
    // some work here
    let cli = redis::Client::open(&format!("redis://127.0.0.1:{}", port)[..]).unwrap();
    let con = cli.get_connection().unwrap();

    let mut result: redis::RedisResult<u8>;
    // first requests returns ResponseError
    result = con.del("my_zset");
    assert_eq!(result.unwrap_err().kind(), redis::ErrorKind::ResponseError);
    // from now on it's IoError due to the closed connection
    result = con.del("my_zset");
    assert_eq!(result.unwrap_err().kind(), redis::ErrorKind::IoError);

    child.join().unwrap().unwrap();
}

use std::io;

use futures::{future, Future};

use partial_io::{GenWouldBlock, PartialAsyncRead, PartialWithErrors};
use tokio_core::reactor;

use redis::Value; 

#[derive(Clone, Debug)]
struct ArbitraryValue(Value);
impl ::quickcheck::Arbitrary for ArbitraryValue {
    fn arbitrary<G : ::quickcheck::Gen>(g: &mut G) -> Self {
        let size = g.size();
        ArbitraryValue(arbitrary_value(g, size))
    }
    fn shrink(&self) -> Box<Iterator<Item=Self>> {
        match self.0 {
            Value::Nil | Value::Okay => Box::new(None.into_iter()),
            Value::Int(i) => Box::new(i.shrink().map(Value::Int).map(ArbitraryValue)),
            Value::Data(ref xs) => Box::new(xs.shrink().map(Value::Data).map(ArbitraryValue)),
            Value::Bulk(ref xs) => {
                let ys = xs.iter().map(|x| ArbitraryValue(x.clone())).collect::<Vec<_>>();
                Box::new(ys.shrink().map(|xs| xs.into_iter().map(|x| x.0).collect()).map(Value::Bulk).map(ArbitraryValue))
            }
            Value::Status(ref status) => Box::new(status.shrink().map(Value::Status).map(ArbitraryValue)),
        }
    }
}

fn arbitrary_value<G : ::quickcheck::Gen>(g: &mut G, recursive_size: usize) -> Value {
    use quickcheck::Arbitrary;
    if recursive_size == 0 {
        Value::Nil
    } else {
        match g.gen_range(0, 6) {
            0 => Value::Nil,
            1 => Value::Int(Arbitrary::arbitrary(g)),
            2 => Value::Data(Arbitrary::arbitrary(g)),
            3 => {
                let size = { let s = g.size(); g.gen_range(0, s) };
                Value::Bulk((0..size).map(|_| arbitrary_value(g, recursive_size / size)).collect())
            }
            4 => {
                let size = { let s = g.size(); g.gen_range(0, s) };
                Value::Status(g.gen_ascii_chars().take(size).collect())
            }
            5 => Value::Okay,
            _ => unreachable!(),
        }
    }
}

#[doc(hidden)]
pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()> where W: io::Write {
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{}\r\n", val),
        Value::Data(ref val) => {
            try!(write!(writer, "${}\r\n", val.len()));
            writer.write_all(val)
        }
        Value::Bulk(ref values) => {
            try!(write!(writer, "*{}\r\n", values.len()));
            for val in values.iter() {
                try!(encode_value(val, writer));
            }
            Ok(())
        }
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::Status(ref s) => write!(writer, "+{}\r\n", s),
    }
}

quickcheck!{
    fn partial_io_parse(input: ArbitraryValue, seq: PartialWithErrors<GenWouldBlock>) -> () {
        println!("{:?}", input);
        let input = ArbitraryValue(Value::Bulk(vec![]));
        let seq = vec![partial_io::PartialOp::Limited(2)];

        let mut encoded_input = Vec::new();
        encode_value(&input.0, &mut encoded_input).unwrap();

        let reader = io::Cursor::new(&encoded_input[..]);
        let partial_reader = PartialAsyncRead::new(reader, seq);
        let mut core = reactor::Core::new().unwrap();

        let timeout = reactor::Timeout::new(Duration::from_secs(1), &core.handle())
            .unwrap()
            .then(|_| future::err(redis::RedisError::from((redis::ErrorKind::IoError, "Timeout"))));
        let result = core.run(redis::parse_async(BufReader::new(partial_reader)).select(timeout))
            .map(|t| (t.0).1)
            .map_err(|e| e.0);

        assert!(result.as_ref().is_ok(), "{}", result.unwrap_err());
        assert_eq!(
            result.unwrap(),
            input.0,
        );
    }
}
