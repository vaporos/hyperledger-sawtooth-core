
extern crate zmq;
extern crate protobuf;

use processor;
use protobuf::Message;

pub struct TransactionProcessor<'a, 'b> {
    endpoint: &'a str,
    handlers: Vec<&'b TransactionHandler<'b>>
}

pub trait TransactionHandler<'a> {
    fn family_name(&'a self) -> &'a String;
    fn family_versions(&'a self) -> &'a Vec<String>;
    fn encodings(&'a self) -> &'a Vec<String>;
    fn namespaces(&'a self) -> &'a Vec<String>;
}

impl<'a, 'b> TransactionProcessor<'a, 'b> {
    pub fn new(endpoint: &str) -> TransactionProcessor {
        TransactionProcessor {
            endpoint: endpoint,
            handlers: Vec::new()
        }
    }

    pub fn add_handler(&mut self, handler: &'b TransactionHandler<'b>) {
        self.handlers.push(handler);
    }

    pub fn start(&self) {
        println!("connecting to endpoint: {}", self.endpoint);

        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::REQ).unwrap();
        socket.set_identity("foobar".as_bytes());
        socket.connect(self.endpoint).unwrap();

        for handler in &self.handlers {
            for version in handler.family_versions() {
                for encoding in handler.encodings() {
                    let mut request = processor::TpRegisterRequest::new();
                    request.set_family(handler.family_name().clone());
                    request.set_version(version.clone());
                    request.set_encoding(encoding.clone());
                    println!("sending TpRegisterRequest: {} {} {}",
                             &handler.family_name(),
                             &version,
                             &encoding);
                    let serialized = request.write_to_bytes().unwrap();
                    let x : &[u8] = &serialized;
                    socket.send_multipart(&[x], 0).unwrap();
                }
            }
        }
    }
}
