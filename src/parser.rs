use std::any::Any;
use std::str;
use std::io::{BufRead, BufReader};

use types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use combine::{self, choice, Stream};
use combine::primitives::RangeStream;
#[allow(unused_imports)] // See https://github.com/rust-lang/rust/issues/43970
use combine::primitives::StreamError;
use combine::byte::{byte, crlf, newline};
use combine::range::{take, take_until_range, recognize};

use futures::{Async, Future, Poll};
use tokio_io::AsyncRead;

struct ResultExtend(Result<Vec<Value>, RedisError>);

impl Default for ResultExtend {
    fn default() -> Self {
        ResultExtend(Ok(Vec::new()))
    }
}

// TODO Remove
impl ::std::iter::FromIterator<RedisResult<Value>> for ResultExtend {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = RedisResult<Value>>,
    {
        let mut result = Self::default();
        result.extend(iter);
        result
    }
}

impl Extend<RedisResult<Value>> for ResultExtend {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = RedisResult<Value>>,
    {
        let mut returned_err = None;
        match self.0 {
            Ok(ref mut elems) => for item in iter {
                match item {
                    Ok(item) => elems.push(item),
                    Err(err) => {
                        returned_err = Some(err);
                        break;
                    }
                }
            },
            Err(_) => (),
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

parser!{
    type PartialState = Option<Box<Any>>;
    fn value['a, I]()(I) -> RedisResult<Value>
        where [I: Stream<Item = u8, Range = &'a [u8], Error = combine::easy::StreamErrors<I>> + RangeStream,
               // FIXME This shouldn't be necessary but rustc is currently unable to figure out
               // this type
               I::Error: combine::primitives::ParseError<I::Item, I::Range, I::Position, StreamError = combine::easy::Error<I::Item, I::Range>> ]
    {
        let end_of_line: fn () -> _ = || crlf().or(newline());
        let line = || recognize(take_until_range(&b"\r\n"[..]).with(end_of_line()))
            .and_then(|line: &[u8]| str::from_utf8(&line[..line.len() - 2]).map_err(combine::easy::Error::other));

        let status = || line().map(|line| {
            if line == "OK" {
                Value::Okay
            } else {
                Value::Status(line.into())
            }
        });

        let int = || line().and_then(|line| {
            match line.trim().parse::<i64>() {
                Err(_) => Err(combine::easy::Error::message_static_message("Expected integer, got garbage")),
                Ok(value) => Ok(value),
            }
        });

        let data = || int().then_partial(move |size| {
            if *size < 0 {
                combine::value(Value::Nil).left()
            } else {
                take(*size as usize)
                    .map(|bs: &[u8]| Value::Data(bs.to_vec()))
                    .skip(end_of_line())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(|&mut length| {
                if length < 0 {
                    combine::value(Value::Nil).map(Ok).left()
                } else {
                    let length = length as usize;
                    combine::count_min_max(length, length, value()).map(|result: ResultExtend| {
                        result.0.map(Value::Bulk)
                    }).right()
                }
            })
        };

        let error = || {
            line()
                .map(|line: &str| {
                    let desc = "An error was signalled by the server";
                    let mut pieces = line.splitn(2, ' ');
                    let kind = match pieces.next().unwrap() {
                        "ERR" => ErrorKind::ResponseError,
                        "EXECABORT" => ErrorKind::ExecAbortError,
                        "LOADING" => ErrorKind::BusyLoadingError,
                        "NOSCRIPT" => ErrorKind::NoScriptError,
                        code => {
                            return make_extension_error(code, pieces.next())
                        }
                    };
                    match pieces.next() {
                        Some(detail) => RedisError::from((kind, desc, detail.to_string())),
                        None => RedisError::from(((kind, desc))),
                    }
                })
        };

        choice((
           byte(b'+').with(status().map(Ok)),
           byte(b':').with(int().map(Value::Int).map(Ok)),
           byte(b'$').with(data().map(Ok)),
           byte(b'*').with(bulk()),
           byte(b'-').with(error().map(Err))
        ))
    }
}

pub struct ValueFuture<R> {
    reader: Option<R>,
    state: Option<Box<Any>>,
    remaining: Vec<u8>,
    last_buf_len: Option<usize>,
}

impl<R> Future for ValueFuture<R>
where
    R: BufRead,
{
    type Item = (R, Value);
    type Error = RedisError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        assert!(
            self.reader.is_some(),
            "ValueFuture: poll called on completed future"
        );
        let remaining_data = self.remaining.len();

        let (opt, mut removed) = {
            let buffer = try_nb!(self.reader.as_mut().unwrap().fill_buf());
            self.last_buf_len = Some(buffer.len());
            let buffer = if !self.remaining.is_empty() {
                self.remaining.extend(buffer);
                &self.remaining[..]
            } else {
                buffer
            };
            let stream = combine::easy::Stream(combine::primitives::PartialStream(buffer));
            match combine::async::decode(value(), stream, &mut self.state) {
                Ok(x) => x,
                Err(err) => {
                    let err = err.map_position(|pos| pos.translate_position(buffer))
                        .map_range(|range| format!("{:?}", range))
                        .to_string();
                    return Err(RedisError::from(
                        (ErrorKind::ResponseError, "parse error", err),
                    ));
                }
            }
        };

        if !self.remaining.is_empty() {
            self.remaining.drain(..removed);
            if removed >= remaining_data {
                removed = removed - remaining_data;
            } else {
                removed = 0;
            }
        }

        match opt {
            Some(value) => {
                self.reader.as_mut().unwrap().consume(removed);
                let reader = self.reader.take().unwrap();
                return Ok(Async::Ready((reader, value?)));
            }
            None => {
                let buffer_len = {
                    let buffer = try!(self.reader.as_mut().unwrap().fill_buf());
                    self.remaining.extend(&buffer[removed..]);
                    buffer.len()
                };
                self.reader.as_mut().unwrap().consume(buffer_len);
                try_nb!(self.reader.as_mut().unwrap().fill_buf());
                Ok(Async::NotReady)
            }
        }
    }
}

pub fn parse_async<R>(reader: R) -> ValueFuture<R>
where
    R: AsyncRead + BufRead,
{
    ValueFuture {
        reader: Some(reader),
        state: None,
        remaining: Vec::new(),
        last_buf_len: None,
    }
}

fn parse<R>(reader: R) -> RedisResult<Value>
where
    R: BufRead,
{
    let mut parser = ValueFuture {
        reader: Some(reader),
        state: None,
        remaining: Vec::new(),
        last_buf_len: None,
    };
    loop {
        match parser.poll()? {
            Async::NotReady => {
                if parser.last_buf_len == Some(0) {
                    fail!((ErrorKind::ResponseError, "Could not read enough bytes"))
                }
            }
            Async::Ready((_, value)) => return Ok(value),
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
impl<'a, T: BufRead> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    // public api

    pub fn parse_value(&mut self) -> RedisResult<Value> {
        parse(&mut self.reader)
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
