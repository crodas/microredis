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
                [$($tag:tt)+],
                $min_args:expr,
                $key_start:expr,
                $key_stop:expr,
                $key_step:expr,
                $queueable:expr,
            }),+$(,)?
        }),+$(,)?
    }=>  {
        $($(
            #[allow(non_snake_case, non_camel_case_types)]
            pub mod $command {
                //! # Command mod
                //!
                //! Each individual command is defined in their own namespace
                use super::*;
                use async_trait::async_trait;
                use metered::measure;

                /// Command definition
                #[derive(Debug)]
                pub struct Command {
                    tags: &'static [&'static str],
                    min_args: i32,
                    key_start: i32,
                    key_stop: i32,
                    key_step: usize,
                    metrics: Metrics,
                }

                impl Command {
                    /// Creates a new comamnd
                    pub fn new() -> Self {
                        Self {
                            tags: &[$($tag,)+],
                            min_args: $min_args,
                            key_start: $key_start,
                            key_stop: $key_stop,
                            key_step: $key_step,
                            metrics: Metrics::default(),
                        }
                    }
                }

                #[async_trait]
                impl ExecutableCommand for Command {
                    async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
                        let metrics = self.metrics();
                        let hit_count = &metrics.hit_count;
                        let error_count = &metrics.error_count;
                        let in_flight = &metrics.in_flight;
                        let response_time = &metrics.response_time;
                        let throughput = &metrics.throughput;

                        let status = conn.status();
                        if status == ConnectionStatus::Multi && self.is_queueable() {
                            conn.queue_command(args);
                            conn.tx_keys(self.get_keys(args));
                            return Ok(Value::Queued);
                        } else if status == ConnectionStatus::Pubsub && ! self.is_pubsub_executable() {
                            return Err(Error::PubsubOnly(stringify!($command).to_owned()));
                        }

                        measure!(hit_count, {
                            measure!(response_time, {
                                measure!(throughput, {
                                    measure!(in_flight, {
                                        measure!(error_count, $handler(conn, args).await)
                                    })
                                })
                            })
                        })
                    }

                    fn metrics(&self) -> &Metrics {
                        &self.metrics
                    }

                    fn is_pubsub_executable(&self) -> bool {
                        stringify!($ns) == "pubsub" || stringify!($command) == "ping" || stringify!($command) == "reset"
                    }

                    fn is_queueable(&self) -> bool {
                        $queueable
                    }

                    fn get_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a Bytes> {
                        let start = self.key_start;
                        let stop  = if self.key_stop > 0 {
                            self.key_stop
                        } else {
                            (args.len() as i32) + self.key_stop
                        };

                        if start == 0 {
                            return vec![];
                        }

                        let mut result = vec![];

                        for i in (start .. stop+1).step_by(self.key_step) {
                            result.push(&args[i as usize]);
                        }

                        result
                    }

                    fn check_number_args(&self, n: usize) -> bool {
                        if ($min_args >= 0) {
                            n == ($min_args as i32).try_into().unwrap_or(0)
                        } else {
                            let s: usize = ($min_args as i32).abs().try_into().unwrap_or(0);
                            n >= s
                        }
                    }

                    fn group(&self) -> &'static str {
                        stringify!($ns)
                    }

                    fn name(&self) -> &'static str {
                        stringify!($command)
                    }
                }
            }
        )+)+

        use async_trait::async_trait;
        use metered::{Throughput, HitCount, ErrorCount, InFlight, ResponseTime};

        /// Executable command trait
        #[async_trait]
        pub trait ExecutableCommand {
            /// Call the command handler
            async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error>;

            /// Returns a reference to the metrics
            fn metrics(&self) -> &Metrics;

            /// Can this command be queued in a transaction or should it be executed right away?
            fn is_queueable(&self) -> bool;

            /// Can this command be executed in a pub-sub only mode?
            fn is_pubsub_executable(&self) -> bool;

            /// Returns all database keys from the command arguments
            fn get_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a Bytes>;

            /// Checks if a given number of args is expected by this command
            fn check_number_args(&self, n: usize) -> bool;

            /// Command group
            fn group(&self) -> &'static str;

            /// Command name
            fn name(&self) -> &'static str;
        }

        /// Metric struct for all command
        #[derive(Debug, Default, serde::Serialize)]
        pub struct Metrics {
            hit_count: HitCount,
            error_count: ErrorCount,
            in_flight: InFlight,
            response_time: ResponseTime,
            throughput: Throughput,
        }

        /// Metrics for all defined commands
        #[derive(serde::Serialize)]
        pub struct ServiceMetricRegistry<'a> {
            $($(
            $command: &'a Metrics,
            )+)+
        }

        /// Dispatcher struct
        ///
        /// The list of commands are generated in this strucutre by this macro.
        #[allow(non_snake_case, non_camel_case_types)]
        #[derive(Debug)]
        pub struct Dispatcher {
            $($(
                $command: $command::Command,
            )+)+
        }

        impl Dispatcher {
            /// Creates a new dispatcher.
            pub fn new() -> Self {
                Self {
                    $($(
                        $command: $command::Command::new(),
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
            pub fn get_all_commands(&self) -> Vec<&(dyn ExecutableCommand + Send + Sync + 'static)> {
                vec![
                $($(
                    &self.$command,
                )+)+
                ]
            }

            /// Returns a command handler for a given command
            pub fn get_handler_for_command(&self, command: &str) -> Result<&(dyn ExecutableCommand + Send + Sync + 'static), Error> {
                match command {
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
            pub fn get_handler(&self, args: &[Bytes]) -> Result<&(dyn ExecutableCommand + Send + Sync + 'static), Error> {
                let command = String::from_utf8_lossy(&args[0]).to_lowercase();
                let command = self.get_handler_for_command(&command)?;
                if ! command.check_number_args(args.len()) {
                    Err(Error::InvalidArgsCount(command.name().into()))
                } else {
                    Ok(command)
                }
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

/// Convert a stream to a Bytes
#[macro_export]
macro_rules! bytes {
    ($content:tt) => {
        Bytes::from(&$content[..])
    };
}
