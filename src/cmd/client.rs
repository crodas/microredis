use crate::{connection::Connection, error::Error, option, value::Value};
use bytes::Bytes;

pub fn client(conn: &mut Connection, args: &[Bytes]) -> Result<Value, Error> {
    let sub = unsafe { std::str::from_utf8_unchecked(&args[1]) }.to_string();

    let expected = match sub.to_lowercase().as_str() {
        "setname" => 3,
        _ => 2,
    };

    if args.len() != expected {
        return Err(Error::WrongArgument(
            "client".to_owned(),
            sub.to_uppercase(),
        ));
    }

    match sub.to_lowercase().as_str() {
        "id" => Ok((conn.id() as i64).into()),
        "info" => Ok(conn.info().as_str().into()),
        "getname" => Ok(option!(conn.name().to_owned())),
        "setname" => {
            let name = unsafe { std::str::from_utf8_unchecked(&args[2]) }.to_string();
            conn.set_name(name);
            Ok(Value::OK)
        }
        _ => Err(Error::WrongArgument(
            "client".to_owned(),
            sub.to_uppercase(),
        )),
    }
}

pub fn echo(_conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    Ok(Value::Blob(args[1].to_owned()))
}

pub fn ping(_conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
    match args.len() {
        1 => Ok(Value::String("PONG".to_owned())),
        2 => Ok(Value::Blob(args[1].to_owned())),
        _ => Err(Error::InvalidArgsCount("ping".to_owned())),
    }
}
