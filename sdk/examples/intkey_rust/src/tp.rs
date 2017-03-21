
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

        for handler in &self.handlers {
            for version in handler.family_versions() {
                for encoding in handler.encodings() {
                    println!("  handler: {} {} {}",
                             &handler.family_name(),
                             &version,
                             &encoding);
                }
            }
        }
    }
}
