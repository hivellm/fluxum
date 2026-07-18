//! `fluxum` command-line binary.

fn main() {
    let code = fluxum_cli::run(std::env::args().skip(1));
    std::process::exit(code);
}
