use http::request::Builder;
use std::collections::HashMap;

pub enum HeaderField {
    User(String),
    Source(String),
    Catalog(String),
    Schema(String),
    // TimeZone,  // Not yet supported
    // Language,  // Not yet supported
    TraceToken(String),
    Session(HashMap<String, String>),
    // Role,  // Not yet supported
    // PreparedStatement,  // Not yet sypported
    TransactionId(String),
    ClientInfo(String),
    ClientTag(String),
    // ResourceEstimate, // Not yet supported
    // ExtraCredential, // Not yet supported
}

impl HeaderField {
    fn get_key(&self) -> &'static str {
        match self {
            HeaderField::User(_) => "User",
            HeaderField::Source(_) => "Source",
            HeaderField::Catalog(_) => "Catalog",
            HeaderField::Schema(_) => "Schema",
            HeaderField::TraceToken(_) => "Trace-Token",
            HeaderField::Session(_) => "Session",
            HeaderField::TransactionId(_) => "Transaction-Id",
            HeaderField::ClientInfo(_) => "Client-Info",
            HeaderField::ClientTag(_) => "Client-Tags",
        }
    }
}

pub struct HeaderBuilder {
    headers: Vec<HeaderField>,
}
impl HeaderBuilder {
    pub fn new() -> Self {
        HeaderBuilder {
            headers: Vec::<HeaderField>::new(),
        }
    }
    pub fn add_header(mut self, field: HeaderField) -> Self {
        self.headers.push(field);
        self
    }
    fn get_prefix() -> &'static str {
        "X-Trino-"
    }
    fn serialize_session(session: &HashMap<String, String>) -> String {
        let mut c = session
            .iter()
            .map(|(key, val)| format!("{}={}", key, val))
            .collect::<Vec<String>>();
        c.sort(); // to easily test this serialization

        let serialized = c.join(",");
        serialized
    }
    pub fn set_headers(&self, mut builder: Builder) -> Builder {
        for header in &self.headers {
            let key = format!("{}{}", Self::get_prefix(), header.get_key());
            let val: String = match header {
                HeaderField::User(val) => val.to_owned(),
                HeaderField::Source(val) => val.to_owned(),
                HeaderField::Catalog(val) => val.to_owned(),
                HeaderField::Schema(val) => val.to_owned(),
                HeaderField::TraceToken(val) => val.to_owned(),
                HeaderField::Session(val) => Self::serialize_session(val),
                HeaderField::TransactionId(val) => val.to_owned(),
                HeaderField::ClientInfo(val) => val.to_owned(),
                HeaderField::ClientTag(val) => val.to_owned(),
            };
            builder = builder.header(key, val);
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use hyper::Request;

    #[test]
    fn test_set_headers_with_all_simple_fields() {
        let builder = Request::builder();
        let r = HeaderBuilder::new()
            .add_header(HeaderField::User("test user".to_string()))
            .add_header(HeaderField::Source("test source".to_string()))
            .add_header(HeaderField::Catalog("test catalog".to_string()))
            .add_header(HeaderField::Schema("test schema".to_string()))
            .add_header(HeaderField::TraceToken("test token".to_string()))
            .add_header(HeaderField::TransactionId("test id".to_string()))
            .add_header(HeaderField::ClientInfo("test info".to_string()))
            .add_header(HeaderField::ClientTag("test tag".to_string()))
            .set_headers(builder)
            .body(())
            .unwrap();

        assert_eq!(
            r.headers().get("X-Trino-User").unwrap(),
            &HeaderValue::from_str("test user").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Source").unwrap(),
            &HeaderValue::from_str("test source").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Catalog").unwrap(),
            &HeaderValue::from_str("test catalog").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Schema").unwrap(),
            &HeaderValue::from_str("test schema").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Trace-Token").unwrap(),
            &HeaderValue::from_str("test token").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Transaction-Id").unwrap(),
            &HeaderValue::from_str("test id").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Client-Info").unwrap(),
            &HeaderValue::from_str("test info").unwrap()
        );
        assert_eq!(
            r.headers().get("X-Trino-Client-Tags").unwrap(),
            &HeaderValue::from_str("test tag").unwrap()
        );
    }

    #[test]
    fn test_set_headers_with_session() {
        let builder = Request::builder();
        let r = HeaderBuilder::new()
            .add_header(HeaderField::Session(HashMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ])))
            .set_headers(builder)
            .body(())
            .unwrap();

        assert_eq!(
            r.headers().get("X-Trino-Session").unwrap(),
            &HeaderValue::from_str("key1=value1,key2=value2").unwrap()
        );
    }
}
