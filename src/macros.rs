#[macro_export]
macro_rules! kv_debug {
    ($($arg: tt)*) => (
        println!("Debug({}:{}): {}", file!(), line!(),
                format_args!($($arg)*));
    )
}

#[macro_export]
macro_rules! kv_note {
    ($($arg: tt)*) => (
        println!("Note({}:{}): {}", file!(), line!(),
                format_args!($($arg)*));
    )
}