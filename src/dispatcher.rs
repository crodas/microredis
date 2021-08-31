use crate::{dispatcher, value::Value};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

fn do_time(_args: &[Value]) -> Result<Value, String> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = format!("{}", since_the_epoch.as_secs());
    let millis = format!("{}", since_the_epoch.subsec_millis());

    Ok(vec![seconds.as_str(), millis.as_str()].into())
}

fn do_command(_args: &[Value]) -> Result<Value, String> {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let in_ms: i128 =
        since_the_epoch.as_secs() as i128 * 1000 + since_the_epoch.subsec_millis() as i128;
    Ok(format!("{}", in_ms).as_str().into())
}

dispatcher! {
    command  {
        do_command,
        ["random" "loading" "stale"],
    },
    get {
        do_command,
        ["random" "loading" "stale"],
    },
    time {
        do_time,
        ["random" "loading" "stale"],
    },
}
