use crate::{db::Db, dispatcher, error::Error, value::Value};
use std::convert::TryInto;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

fn do_time(_db: &Db, _args: &[Value]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = format!("{}", since_the_epoch.as_secs());
    let millis = format!("{}", since_the_epoch.subsec_millis());

    Ok(vec![seconds.as_str(), millis.as_str()].into())
}

fn do_command(_db: &Db, _args: &[Value]) -> Result<Value, Error> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let in_ms: i128 =
        since_the_epoch.as_secs() as i128 * 1000 + since_the_epoch.subsec_millis() as i128;
    Ok(format!("{}", in_ms).as_str().into())
}

fn get(db: &Db, args: &[Value]) -> Result<Value, Error> {
    db.get(&args[1])
}

fn set(db: &Db, args: &[Value]) -> Result<Value, Error> {
    db.set(&args[1], &args[2])
}

dispatcher! {
    command  {
        do_command,
        ["random" "loading" "stale"],
        1,
    },
    get {
        get,
        ["random" "loading" "stale"],
        2,
    },
    set {
        set,
        ["random" "loading" "stale"],
        -3,
    },
    time {
        do_time,
        ["random" "loading" "stale"],
        1,
    },
}
