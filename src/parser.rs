use std::io::{self, BufRead, BufReader};
use std::str;

use types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use bytes::BytesMut;
use futures::{Async, Future, Poll};
use tokio_io::codec::{Decoder, Encoder};
use tokio_io::AsyncRead;

use combine;
use combine::byte::{byte, crlf};
use combine::combinator::{any_send_partial_state, AnySendPartialState, parser as fn_parser};
#[allow(unused_imports)] // See https://github.com/rust-lang/rust/issues/43970
use combine::error::{StreamError, Consumed};
use combine::parser::choice::choice;
use combine::range::{recognize, take, take_until_range};
use combine::stream::{RangeStream, StreamErrorFor};
use combine::stream::user_state::StateStream;

struct ResultExtend<T, E>(Result<T, E>);

impl<T, E> Default for ResultExtend<T, E>
where
    T: Default,
{
    fn default() -> Self {
        ResultExtend(Ok(T::default()))
    }
}

impl<T, U, E> Extend<Result<U, E>> for ResultExtend<T, E>
where
    T: Extend<U>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<U, E>>,
    {
        let mut returned_err = None;
        match self.0 {
            Ok(ref mut elems) => elems.extend(iter.into_iter().scan((), |_, item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    returned_err = Some(err);
                    None
                }
            })),
            Err(_) => (),
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

trait RedisParser {
    fn nil(&mut self) -> RedisResult<()>;
    fn data(&mut self, data: &[u8]) -> RedisResult<()>;
    fn status(&mut self, msg: & str) -> RedisResult<()>;
    fn bulk_start(&mut self, size: usize);
    fn bulk_end(&mut self);
    fn int(&mut self, i: i64) -> RedisResult<()>;
}

enum ValueParser {
    Bulk(Vec<Vec<Value>>),
    Value(Value)
}

impl Default for ValueParser {
     fn default() -> Self {
         ValueParser::Value(Value::Nil)
     }
}

impl ValueParser {
    fn take(&mut self) -> Value {
        let value = ::std::mem::replace(self, ValueParser::default());
        match value {
            ValueParser::Value(v) => v,
            ValueParser::Bulk(_) => unreachable!(),
        }
    }

    fn value(&mut self, value: Value) -> RedisResult<()> {
        match self {
            ValueParser::Value(_) => *self = ValueParser::Value(value),
            ValueParser::Bulk(bulk) => bulk.last_mut().unwrap().push(value),
        }
        Ok(())
    }
}

impl RedisParser for ValueParser {
    fn nil(&mut  self) -> RedisResult<()> {
        self.value(Value::Nil)
    }
    fn data(&mut  self, data: &[u8]) -> RedisResult<()> {
        self.value(Value::Data(data.to_owned()))
    }
    fn status(&mut  self, msg: &str) -> RedisResult<()> {
        self.value(if msg == "OK" {
            Value::Okay
        } else {
            Value::Status(msg.to_owned())
        })
    }
    fn bulk_start(&mut self, size: usize) {
        let bulks = match self {
            ValueParser::Value(_) => {
                *self = ValueParser::Bulk(vec![]);
                match self {
                    ValueParser::Value(_) => unreachable!(),
                    ValueParser::Bulk(bulks) => bulks,
                }
            }
            ValueParser::Bulk(bulks) => bulks,
        };
        bulks.push(Vec::with_capacity(size));
    }
    fn bulk_end(&mut self) {
        *self = match self {
            ValueParser::Value(_) => unreachable!(),
            ValueParser::Bulk(bulks) => {
                let done_bulk = bulks.pop().unwrap();
                match bulks.last_mut() {
                    Some(bulk) => {
                        bulk.push(Value::Bulk(done_bulk));
                        return;
                    }
                    None => ValueParser::Value(Value::Bulk(done_bulk)),
                }
            }
        }
    }
    fn int(&mut  self, i: i64) -> RedisResult<()> {
        self.value(Value::Int(i))
    }
}

parser! {
fn with_state[S, R, I, F](f: F)(StateStream<I, S>) -> R
    where [
        F: FnMut(&mut S) -> R,
        I: combine::Stream
    ]
{
    fn_parser(move |input: &mut StateStream<I, S>| -> Result<_, _> {
        Ok((f(&mut input.state), Consumed::Empty(())))
    })
}
}

parser! {
    type PartialState = AnySendPartialState;
    fn value['a, 'b, I]()(StateStream<I, &'b mut RedisParser>) -> RedisResult<()>
        where [I: RangeStream<Item = u8, Range = &'a [u8]> ]
    {
        let end_of_line: fn () -> _ = || crlf();
        let line = || recognize(take_until_range(&b"\r\n"[..]).with(end_of_line()))
            .and_then(|line: &[u8]| {
                str::from_utf8(&line[..line.len() - 2])
                    .map_err(StreamErrorFor::<I>::other)
            });

        let status = || line().map_input(move |line, input: &mut StateStream<_, &mut RedisParser>| {
            input.state.status(line)
        });

        let int = || line().and_then(move |line| {
            match line.trim().parse::<i64>() {
                Err(_) => Err(StreamErrorFor::<I>::message_static_message("Expected integer, got garbage")),
                Ok(value) => Ok(value),
            }
        });

        let data = || int().then_partial(move |&mut size| {
            if size < 0 {
                with_state(move |state: &mut &mut RedisParser| state.nil())
                    .left()
            } else {
                take(size as usize)
                    .map_input(move |bs: &[u8], input: &mut StateStream<_, &mut RedisParser>| 
                        input.state.data(bs)
                    )
                    .skip(end_of_line())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(move |&mut length| {
                if length < 0 {
                    with_state(move |state: &mut &mut RedisParser| 
                        state.nil()
                    )
                        .left()
                } else {
                    let length = length as usize;
                    with_state(move |state: &mut &mut RedisParser| state.bulk_start(length))
                        .with(combine::count_min_max(length, length, value()))
                        .skip(with_state(|state: &mut &mut RedisParser| state.bulk_end()))
                        .map(|result: ResultExtend<(), _>| {
                            result.0
                        })
                        .right()
                }
            })
        };

        let error = || {
            line()
                .map(move |line: &str| {
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
                        None => RedisError::from((kind, desc)),
                    }
                })
        };

        any_send_partial_state(choice((
           byte(b'+').with(status()),
           byte(b':').with(int().then_partial(move |&mut i| {

                with_state(move |state: &mut &mut RedisParser| 
                    state.int(i)
                )
           })),
           byte(b'$').with(data()),
           byte(b'*').with(bulk()),
           byte(b'-').with(error().map(Err))
        )))
    }
}

#[derive(Default)]
pub struct ValueCodec {
    state: AnySendPartialState,
    value_state: ValueParser,
}

impl Encoder for ValueCodec {
    type Item = Vec<u8>;
    type Error = RedisError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(item);
        Ok(())
    }
}

impl Decoder for ValueCodec {
    type Item = Value;
    type Error = RedisError;
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (opt, removed_len) = {
            let buffer = &bytes[..];
            let result = {
                let stream = StateStream {
                    stream: combine::easy::Stream(combine::stream::PartialStream(buffer)),
                    state: &mut self.value_state as &mut RedisParser
                };
                combine::stream::decode(value(), stream, &mut self.state)
            };
            match result {
                Ok((opt, removed)) => {
                    let value_state = &mut self.value_state;
                    (opt.map(|result| result.map(|()| value_state.take())), removed)
                }
                Err(err) => {
                    let err = err
                        .map_position(|pos| pos.translate_position(buffer))
                        .map_range(|range| format!("{:?}", range))
                        .to_string();
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "parse error",
                        err,
                    )));
                }
            }
        };

        bytes.split_to(removed_len);
        match opt {
            Some(result) => Ok(Some(result?)),
            None => Ok(None),
        }
    }
}

pub struct ValueFuture<R> {
    reader: Option<R>,
    state: AnySendPartialState,
    value_state: ValueParser,
    // Intermediate storage for data we know that we need to parse a value but we haven't been able
    // to parse completely yet
    remaining: Vec<u8>,
}

impl<R> Future for ValueFuture<R>
where
    R: BufRead,
{
    type Item = (R, Value);
    type Error = RedisError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            assert!(
                self.reader.is_some(),
                "ValueFuture: poll called on completed future"
            );
            let remaining_data = self.remaining.len();

            let (opt, mut removed) = {
                let buffer = try_nb!(self.reader.as_mut().unwrap().fill_buf());
                if buffer.len() == 0 {
                    fail!((ErrorKind::ResponseError, "Could not read enough bytes"))
                }
                let buffer = if !self.remaining.is_empty() {
                    self.remaining.extend(buffer);
                    &self.remaining[..]
                } else {
                    buffer
                };
                let result = {
                    let stream = StateStream {
                        stream: combine::easy::Stream(combine::stream::PartialStream(buffer)),
                        state: &mut self.value_state as &mut RedisParser
                    };
                    combine::stream::decode(value(), stream, &mut self.state)
                };
                match result {
                    Ok((opt, removed)) => {
                        let value_state = &mut self.value_state;
                        (opt.map(|result| result.map(|()| value_state.take())), removed)
                    }
                    Err(err) => {
                        let err = err
                            .map_position(|pos| pos.translate_position(buffer))
                            .map_range(|range| format!("{:?}", range))
                            .to_string();
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "parse error",
                            err,
                        )));
                    }
                }
            };

            if !self.remaining.is_empty() {
                // Remove the data we have parsed and adjust `removed` to be the amount of data we
                // consumed from `self.reader`
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
                    // We have not enough data to produce a Value but we know that all the data of
                    // the current buffer are necessary. Consume all the buffered data to ensure
                    // that the next iteration actually reads more data.
                    let buffer_len = {
                        let buffer = try!(self.reader.as_mut().unwrap().fill_buf());
                        if remaining_data == 0 {
                            self.remaining.extend(&buffer[removed..]);
                        }
                        buffer.len()
                    };
                    self.reader.as_mut().unwrap().consume(buffer_len);
                }
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
        state: Default::default(),
        value_state: ValueParser::default(),
        remaining: Vec::new(),
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
        let mut parser = ValueFuture {
            reader: Some(&mut self.reader),
            state: Default::default(),
            value_state: ValueParser::default(),
            remaining: Vec::new(),
        };
        match parser.poll()? {
            Async::NotReady => Err(io::Error::from(io::ErrorKind::WouldBlock).into()),
            Async::Ready((_, value)) => Ok(value),
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
