//! # Macros
//!
//! All macros are defined in this module

/// Dispatcher macro
///
/// In this macro all command and their definition are defined.
///
/// This macro generates rust code for each command and encapsulates all the logic to fast
/// execution.
///
/// Using macros allow to generate pretty efficient code for run time and easy to extend at
/// writting time.
#[macro_export]
macro_rules! dispatcher {
    {
        $($ns:ident {
            $($command:ident {
                $handler:expr,
                [$($tag:expr)+],
                $min_args:expr,
                $key_start:expr,
                $key_stop:expr,
                $key_step:expr,
                $is_queueable:expr,
            }),+$(,)?
        }),+$(,)?
    }=>  {
        use futures::future::FutureExt;

        /// Metrics for all defined commands
        #[derive(serde::Serialize)]
        pub struct ServiceMetricRegistry<'a> {
            $($(
            $command: &'a command::Metrics,
            )+)+
        }

        /// Dispatcher struct
        ///
        /// The list of commands are generated in this strucutre by this macro.
        #[allow(non_snake_case, non_camel_case_types)]
        #[derive(Debug)]
        pub struct Dispatcher {
            $($(
                $command: command::Command,
            )+)+
        }

        impl Dispatcher {
            /// Creates a new dispatcher.
            pub fn new() -> Self {
                Self {
                    $($(
                        $command: command::Command::new(
                            stringify!($command),
                            stringify!($ns),
                            &[$($tag,)+],
                            $min_args,
                            $key_start,
                            $key_stop,
                            $key_step,
                            $is_queueable,
                        ),
                    )+)+
                }
            }

            /// Returns all metrics objects
            pub fn get_service_metric_registry(&self) -> ServiceMetricRegistry {
                ServiceMetricRegistry {
                    $($(
                        $command: self.$command.metrics(),
                    )+)+
                }
            }

            /// Returns the handlers for defined commands.
            pub fn get_all_commands(&self) -> Vec<&command::Command> {
                vec![
                $($(
                    &self.$command,
                )+)+
                ]
            }

            /// Returns a command handler for a given command
            #[inline(always)]
            pub fn get_handler_for_command(&self, command: &str) -> Result<&command::Command, Error> {
                match command.to_uppercase().as_str() {
                $($(
                    stringify!($command) => Ok(&self.$command),
                )+)+
                    _ => Err(Error::CommandNotFound(command.into())),
                }
            }

            /// Returns the command handler
            ///
            /// Before returning the command handler this function will make sure the minimum
            /// required arguments are provided. This pre-validation ensures each command handler
            /// has fewer logic when reading the provided arguments.
            #[inline(always)]
            pub fn get_handler(&self, args: &[Bytes]) -> Result<&command::Command, Error> {
                let command = String::from_utf8_lossy(&args[0]).to_uppercase();
                let command = self.get_handler_for_command(&command)?;
                if ! command.check_number_args(args.len()) {
                    Err(Error::InvalidArgsCount(command.name().into()))
                } else {
                    Ok(command)
                }
            }

            /// Returns the command handler
            ///
            /// Before returning the command handler this function will make sure the minimum
            /// required arguments are provided. This pre-validation ensures each command handler
            /// has fewer logic when reading the provided arguments.
            #[inline(always)]
            pub fn execute<'a>(&'a self, conn: &'a Connection, args: &'a [Bytes]) -> futures::future::BoxFuture<'a, Result<Value, Error>> {
                async move {
                    let command = match args.get(0) {
                        Some(s) => Ok(String::from_utf8_lossy(s).to_uppercase()),
                        None => Err(Error::EmptyLine),
                    }?;
                    match command.as_str() {
                        $($(
                            stringify!($command) => {
                                //log::info!("Command: {} -> {:?}", stringify!($command), args);
                                let command = &self.$command;
                                    let status = conn.status();
                                if ! command.check_number_args(args.len()) {
                                    if status == ConnectionStatus::Multi {
                                        conn.fail_transaction();
                                    }
                                    Err(Error::InvalidArgsCount(command.name().into()))
                                } else {
                                    let metrics = command.metrics();
                                    let hit_count = &metrics.hit_count;
                                    let error_count = &metrics.error_count;
                                    let in_flight = &metrics.in_flight;
                                    let response_time = &metrics.response_time;
                                    let throughput = &metrics.throughput;

                                    if status == ConnectionStatus::Multi && command.is_queueable() {
                                        conn.queue_command(args);
                                        conn.tx_keys(command.get_keys(args));
                                        return Ok(Value::Queued);
                                    } else if status == ConnectionStatus::FailedTx && command.is_queueable() {
                                        return Ok(Value::Queued);
                                    } else if status == ConnectionStatus::Pubsub && ! command.is_pubsub_executable() {
                                        return Err(Error::PubsubOnly(stringify!($command).to_owned()));
                                    }

                                    metered::measure!(hit_count, {
                                        metered::measure!(response_time, {
                                            metered::measure!(throughput, {
                                                metered::measure!(in_flight, {
                                                    metered::measure!(error_count, $handler(conn, args).await)
                                                })
                                            })
                                        })
                                    })
                                }
                            }
                        )+)+,
                        _ => {
                            if conn.status() == ConnectionStatus::Multi {
                                conn.fail_transaction();
                            }
                            Err(Error::CommandNotFound(command.into()))
                        },
                    }
                }.boxed()
            }
        }
    }
}

/// Generate code for From/Into a type to a Value
#[macro_export]
macro_rules! value_try_from {
    {$type: ty, $value: expr} => {
        impl From<$type> for Value {
            fn from(value: $type) -> Value {
                $value(value.into())
            }
        }

        value_vec_try_from!($type);
    }
}

/// Generate code for From/Into a vec of type to a Value::Array
#[macro_export]
macro_rules! value_vec_try_from {
    {$type: ty} => {
        impl From<Vec<$type>> for Value {
            fn from(value: Vec<$type>) -> Value {
                Value::Array(value.iter().map(|x| (*x).into()).collect())
            }
        }
    }
}

/// Converts an Option<T> to Value. If the option is None Value::Null is returned.
#[macro_export]
macro_rules! option {
    {$type: expr} => {
        if let Some(val) = $type {
            val.into()
        } else {
            Value::Null
        }
    }
}

/// Check if a given command argument in a position $pos is eq to a $command
#[macro_export]
macro_rules! check_arg {
    {$args: tt, $pos: tt, $command: tt} => {{
        match $args.get($pos) {
            Some(bytes) => {
                String::from_utf8_lossy(&bytes).to_uppercase() == $command
            },
            None => false,
        }
    }}
}

/// Reads an argument index. If the index is not provided an Err(Error:Syntax)
/// is thrown
#[macro_export]
macro_rules! try_get_arg {
    {$args: tt, $pos: expr} => {{
        match $args.get($pos) {
            Some(bytes) => bytes,
            None => return Err(Error::Syntax),
        }
    }}
}
/// Reads an argument index as an utf-8 string or return an Error::Syntax
#[macro_export]
macro_rules! try_get_arg_str {
    {$args: tt, $pos: expr} => {{
        String::from_utf8_lossy(try_get_arg!($args, $pos))
    }}
}

/// Convert a stream to a Bytes
#[macro_export]
macro_rules! bytes {
    ($content:tt) => {
        (&$content[..]).into()
    };
}
