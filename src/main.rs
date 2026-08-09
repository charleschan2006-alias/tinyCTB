fn main() {
    if let Err(error) = tinyctb::main_entry() {
        println!("{}", tinyctb::render_error_envelope(&error));
        std::process::exit(1);
    }
}
