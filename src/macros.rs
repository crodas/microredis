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
                use super::*;
                use async_trait::async_trait;

                #[derive(Debug)]
                pub struct Command {
                    pub tags: &'static [&'static str],
                    pub min_args: i32,
                    pub key_start: i32,
                    pub key_stop: i32,
                    pub key_step: usize,
                }

                impl Command {
                    pub fn new() -> Self {
                        Self {
                            tags: &[$($tag,)+],
                            min_args: $min_args,
                            key_start: $key_start,
                            key_stop: $key_stop,
                            key_step: $key_step,
                        }
                    }
                }

                #[async_trait]
                impl ExecutableCommand for Command {
                    async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
                        let status = conn.status();
                        if status == ConnectionStatus::Multi && self.is_queueable() {
                            conn.queue_command(args);
                            conn.tx_keys(self.get_keys(args));
                            Ok(Value::Queued)
                        } else if status == ConnectionStatus::Pubsub && ! self.is_pubsub_executable() {
                            Err(Error::PubsubOnly(stringify!($command).to_owned()))
                        } else {
                            $handler(conn, args).await
                        }
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

        #[async_trait]
        pub trait ExecutableCommand {
            async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error>;

            fn is_queueable(&self) -> bool;

            fn is_pubsub_executable(&self) -> bool;

            fn get_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a Bytes>;

            fn check_number_args(&self, n: usize) -> bool;

            fn group(&self) -> &'static str;

            fn name(&self) -> &'static str;
        }

        #[allow(non_snake_case, non_camel_case_types)]
        #[derive(Debug)]
        pub struct Dispatcher {
            $($(
                $command: $command::Command,
            )+)+
        }

        impl Dispatcher {
            pub fn new() -> Self {
                Self {
                    $($(
                        $command: $command::Command::new(),
                    )+)+
                }
            }
            pub fn get_handler(&self, args: &[Bytes]) -> Result<&(dyn ExecutableCommand + Send + Sync + 'static), Error> {
                let command = String::from_utf8_lossy(&args[0]).to_lowercase();

                let command: &(dyn ExecutableCommand + Send + Sync + 'static) = match command.as_str() {
                $($(
                    stringify!($command) => &self.$command,
                )+)+
                    _ => return Err(Error::CommandNotFound(command.into())),
                };

                if ! command.check_number_args(args.len()) {
                    Err(Error::InvalidArgsCount(command.name().into()))
                } else {
                    Ok(command)
                }
            }
        }
    }
}

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

#[macro_export]
macro_rules! bytes {
    ($content:tt) => {
        Bytes::from(&$content[..])
    };
}
