fn main() {
    match tinyctb::main_entry() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            println!("{}", tinyctb::render_error_envelope(&error));
            std::process::exit(1);
        }
    }
}
