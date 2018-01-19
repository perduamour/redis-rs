use std::io::{BufReader, Read};

use types::{make_extension_error, ErrorKind, RedisResult, Value};


mod combine_parser {
    use std::str;

    use types::{make_extension_error, ErrorKind, RedisResult, Value};

    use combine::{self, Parser, Stream};
    use combine::primitives::RangeStream;
    use combine::byte::{byte, crlf, newline};
    use combine::range::{take, take_while};

    parser!{
        fn value['a, I]()(I) -> Value
            where [I: Stream<Item = u8, Range = &'a [u8]> + RangeStream]
        {
            let end_of_line = || crlf().or(newline());
            let line = || take_while(|c| c != b'\r' && c != b'\n')
                .skip(end_of_line())
                .and_then(|line: &[u8]| str::from_utf8(line));

            let status = || line().map(|line| {
                if line == "OK" {
                    Value::Okay
                } else {
                    Value::Status(line.into())
                }
            });
            let int = || line().and_then(|line| {
                match line.trim().parse::<i64>() {
                    Err(_) => Err(combine::primitives::Error::Message("Expected integer, got garbage".into())),
                    Ok(value) => Ok(value),
                }
            });
            let data = || int().then(|size| take(size as usize).skip(end_of_line()).map(|bs: &[u8]| bs.to_vec()));
            let bulk = || {
                int().then(|length| {
                    // FIXME Don't box the parsers here
                    if length < 0 {
                        combine::value(Value::Nil).boxed()
                    } else {
                        let length = length as usize;
                        combine::count_min_max(length, length, value()).map(Value::Bulk).boxed()
                    }
                })
            };
            let error = || {
                line()
                    .and_then(|line: &str| -> RedisResult<_> {
                        let desc = "An error was signalled by the server";
                        let mut pieces = line.splitn(2, ' ');
                        let kind = match pieces.next().unwrap() {
                            "ERR" => ErrorKind::ResponseError,
                            "EXECABORT" => ErrorKind::ExecAbortError,
                            "LOADING" => ErrorKind::BusyLoadingError,
                            "NOSCRIPT" => ErrorKind::NoScriptError,
                            code => {
                                fail!(make_extension_error(code, pieces.next()))
                            }
                        };
                        match pieces.next() {
                            Some(detail) => fail!((kind, desc, detail.to_string())),
                            None => fail!(((kind, desc))),
                        }
                    })
            };
            choice!(
               byte(b'+').with(status()),
               byte(b':').with(int().map(Value::Int)),
               byte(b'$').with(data().map(Value::Data)),
               byte(b'*').with(bulk()),
               byte(b'-').with(error())
            )
        }
    }
}

/// The internal redis response parser.
pub struct Parser<T> {
    reader: T,
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl<'a, T: Read> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    // public api

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.  If the reader is not yet
    /// ready this will block.
    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let b = try!(self.read_byte());
        match b as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => fail!((
                ErrorKind::ResponseError,
                "Invalid response when parsing value"
            )),
        }
    }

    // internal helpers

    #[inline]
    fn expect_char(&mut self, refchar: char) -> RedisResult<()> {
        if try!(self.read_byte()) as char == refchar {
            Ok(())
        } else {
            fail!((ErrorKind::ResponseError, "Invalid byte in response"));
        }
    }

    #[inline]
    fn expect_newline(&mut self) -> RedisResult<()> {
        match try!(self.read_byte()) as char {
            '\n' => Ok(()),
            '\r' => self.expect_char('\n'),
            _ => fail!((ErrorKind::ResponseError, "Invalid byte in response")),
        }
    }

    fn read_line(&mut self) -> RedisResult<Vec<u8>> {
        let mut rv = vec![];

        loop {
            let b = try!(self.read_byte());
            match b as char {
                '\n' => {
                    break;
                }
                '\r' => {
                    try!(self.expect_char('\n'));
                    break;
                }
                _ => rv.push(b),
            };
        }

        Ok(rv)
    }

    fn read_string_line(&mut self) -> RedisResult<String> {
        match String::from_utf8(try!(self.read_line())) {
            Err(_) => fail!((
                ErrorKind::ResponseError,
                "Expected valid string, got garbage"
            )),
            Ok(value) => Ok(value),
        }
    }

    fn read_byte(&mut self) -> RedisResult<u8> {
        let buf: &mut [u8; 1] = &mut [0];
        let nread = try!(self.reader.read(buf));

        if nread < 1 {
            fail!((ErrorKind::ResponseError, "Could not read enough bytes"))
        } else {
            Ok(buf[0])
        }
    }

    fn read(&mut self, bytes: usize) -> RedisResult<Vec<u8>> {
        let mut rv = vec![0; bytes];
        let mut i = 0;
        while i < bytes {
            let res_nread = {
                let ref mut buf = &mut rv[i..];
                self.reader.read(buf)
            };
            match res_nread {
                Ok(nread) if nread > 0 => i += nread,
                Ok(_) => fail!((ErrorKind::ResponseError, "Could not read enough bytes")),
                Err(e) => return Err(From::from(e)),
            }
        }
        Ok(rv)
    }

    fn read_int_line(&mut self) -> RedisResult<i64> {
        let line = try!(self.read_string_line());
        match line.trim().parse::<i64>() {
            Err(_) => fail!((ErrorKind::ResponseError, "Expected integer, got garbage")),
            Ok(value) => Ok(value),
        }
    }

    fn parse_status(&mut self) -> RedisResult<Value> {
        let line = try!(self.read_string_line());
        if line == "OK" {
            Ok(Value::Okay)
        } else {
            Ok(Value::Status(line))
        }
    }

    fn parse_int(&mut self) -> RedisResult<Value> {
        Ok(Value::Int(try!(self.read_int_line())))
    }

    fn parse_data(&mut self) -> RedisResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let data = try!(self.read(length as usize));
            try!(self.expect_newline());
            Ok(Value::Data(data))
        }
    }

    fn parse_bulk(&mut self) -> RedisResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let mut rv = vec![];
            rv.reserve(length as usize);
            for _ in 0..length {
                rv.push(try!(self.parse_value()));
            }
            Ok(Value::Bulk(rv))
        }
    }

    fn parse_error(&mut self) -> RedisResult<Value> {
        let desc = "An error was signalled by the server";
        let line = try!(self.read_string_line());
        let mut pieces = line.splitn(2, ' ');
        let kind = match pieces.next().unwrap() {
            "ERR" => ErrorKind::ResponseError,
            "EXECABORT" => ErrorKind::ExecAbortError,
            "LOADING" => ErrorKind::BusyLoadingError,
            "NOSCRIPT" => ErrorKind::NoScriptError,
            code => {
                fail!(make_extension_error(code, pieces.next()));
            }
        };
        match pieces.next() {
            Some(detail) => fail!((kind, desc, detail.to_string())),
            None => fail!((kind, desc)),
        }
    }
}


/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new(BufReader::new(bytes));
    parser.parse_value()
}
