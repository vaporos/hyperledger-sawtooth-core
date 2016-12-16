#[macro_use]
extern crate clap;

extern crate crypto;
extern crate rustc_serialize as serialize;

use clap::{Arg, App};
use crypto::sha2;
use crypto::digest::Digest;

mod tp;

fn get_intkey_prefix() -> String {
    let mut sha = sha2::Sha512::new();
    sha.input_str("intkey");
    sha.result_str()[..6].to_string()
}

struct IntkeyTransactionHandler<'a> {
    family_name: String,
    family_versions: Vec<String>,
    encodings: Vec<String>,
    namespace_prefix: &'a str
}

impl<'a> IntkeyTransactionHandler<'a> {
    fn new(namespace_prefix: &str) -> IntkeyTransactionHandler {
        IntkeyTransactionHandler {
            family_name: "intkey".to_string(),
            family_versions: vec!["1.0".to_string()],
            encodings: vec!["application/cbor".to_string()],
            namespace_prefix: namespace_prefix
        }
    }
}

impl<'a> tp::TransactionHandler<'a> for IntkeyTransactionHandler<'a> {
    fn family_name(&'a self) -> &'a String {
        return &self.family_name
    }

    fn family_versions(&'a self) -> &'a Vec<String> {
        return &self.family_versions
    }

    fn encodings(&'a self) -> &'a Vec<String> {
        return &self.encodings
    }
}

fn main() {
    let matches = App::new("Intkey Transaction Processor (Rust)")
        .version(crate_version!())
        .arg(Arg::with_name("ENDPOINT")
             .long("endpoint")
             .default_value("127.0.0.1:40000")
             .help("the connection endpoint"))
        .get_matches();

    let endpoint = matches.value_of("ENDPOINT").unwrap();
    let intkey_prefix = &get_intkey_prefix();

    let handler = IntkeyTransactionHandler::new(intkey_prefix);

    let mut processor = tp::TransactionProcessor::new(endpoint);

    processor.add_handler(&handler);
    processor.start()
}
