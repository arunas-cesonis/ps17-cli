
pub fn setup_tracing(level: tracing_subscriber::filter::LevelFilter) {
    let t = tracing_subscriber::fmt::time::Uptime::default();
    let fmt = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_timer(t)
        .with_thread_ids(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(fmt).unwrap();
}
