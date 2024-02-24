#[derive(Debug, serde::Deserialize)]
pub enum AuthorizationKind {
    QueryParam,
    Header,
}
#[derive(Debug, serde::Deserialize)]
pub struct HttpConfig {
    pub key: String,
    pub host: String,
    pub authorization_kind: AuthorizationKind,
}
