#[macro_export]
macro_rules! dispatcher {
    {
        $($command:ident {
            $handler:ident,
            [$($tag:tt)+],
        },)+$(,)?
    }=>  {
        $(
            #[allow(non_snake_case, non_camel_case_types)]
            pub mod $command {
                use super::*;

                pub struct Command {
                    pub tags: &'static [&'static str],
                }

                impl Command {
                    pub fn new() -> Self {
                        Self {
                            tags: &[$($tag,)+],
                        }
                    }
                }

                impl ExecutableCommand for Command {
                    fn execute(&self, args: &[Value]) -> Result<Value, String> {
                        $handler(args)
                    }

                    fn name(&self) -> &'static str {
                        stringify!($command)
                    }
                }
            }
        )+
        use std::ops::Deref;

        pub trait ExecutableCommand {
            fn execute(&self, args: &[Value]) -> Result<Value, String>;

            fn name(&self) -> &'static str;
        }

        #[allow(non_snake_case, non_camel_case_types)]
        pub enum Dispatcher {
            $(
                $command($command::Command),
            )+
        }

        impl Dispatcher {
            pub fn new(command: &Value) -> Result<Self, String> {
                let command = match command {
                    Value::String(x) => Ok(x.as_str()),
                    Value::Blob(x) => Ok(unsafe { std::str::from_utf8_unchecked(&x) }),
                    _ => Err("Invalid type"),
                }?;

                match command.to_lowercase().as_str() {
                $(
                    stringify!($command) => Ok(Self::$command($command::Command::new())),
                )+
                    _ => Err(format!("Command ({}) not found", command)),
                }
            }
        }

        impl Deref for Dispatcher {
            type Target = dyn ExecutableCommand + Sync + Send;

            fn deref(&self) -> &(dyn ExecutableCommand + Sync + Send + 'static) {
                match self {
                    $(
                        Self::$command(v) => v as &(dyn ExecutableCommand + Sync + Send),
                    )+
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
