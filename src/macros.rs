#[macro_export]
macro_rules! dispatcher {
    {
        $($ns:ident {
            $($command:ident {
                $handler:expr,
                [$($tag:tt)+],
                $min_args:expr,
            }),+$(,)?
        }),+$(,)?
    }=>  {
        $($(
            #[allow(non_snake_case, non_camel_case_types)]
            pub mod $command {
                use super::*;
                use async_trait::async_trait;

                pub struct Command {
                    pub tags: &'static [&'static str],
                    pub min_args: i32,
                }

                impl Command {
                    pub fn new() -> Self {
                        Self {
                            tags: &[$($tag,)+],
                            min_args: $min_args,
                        }
                    }
                }

                #[async_trait]
                impl ExecutableCommand for Command {
                    async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error> {
                        $handler(conn, args).await
                    }

                    fn check_number_args(&self, n: usize) -> bool {
                        if ($min_args >= 0) {
                            n == ($min_args as i32).try_into().unwrap()
                        } else {
                            let s: usize = ($min_args as i32).abs().try_into().unwrap();
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
        use std::ops::Deref;
        use async_trait::async_trait;

        #[async_trait]
        pub trait ExecutableCommand {
            async fn execute(&self, conn: &Connection, args: &[Bytes]) -> Result<Value, Error>;

            fn check_number_args(&self, n: usize) -> bool;

            fn group(&self) -> &'static str;

            fn name(&self) -> &'static str;
        }

        #[allow(non_snake_case, non_camel_case_types)]
        pub enum Dispatcher {
            $($(
                $command($command::Command),
            )+)+
        }

        impl Dispatcher {
            pub fn new(args: &[Bytes]) -> Result<Self, Error> {
                let command = unsafe { std::str::from_utf8_unchecked(&args[0]) };

                let command = match command.to_lowercase().as_str() {
                $($(
                    stringify!($command) => Ok(Self::$command($command::Command::new())),
                )+)+
                    _ => Err(Error::CommandNotFound(command.into())),
                }?;

                if ! command.check_number_args(args.len()) {
                    Err(Error::InvalidArgsCount(command.name().into()))
                } else {
                    Ok(command)
                }
            }
        }

        impl Deref for Dispatcher {
            type Target = dyn ExecutableCommand + Sync + Send;

            fn deref(&self) -> &(dyn ExecutableCommand + Sync + Send + 'static) {
                match self {
                    $($(
                        Self::$command(v) => v as &(dyn ExecutableCommand + Sync + Send),
                    )+)+
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
                let command = unsafe { std::str::from_utf8_unchecked(&bytes) };
                command.to_uppercase() == $command
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
