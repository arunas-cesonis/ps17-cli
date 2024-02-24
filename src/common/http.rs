use crate::arrow2::{parse_response, schema3};
use crate::http_config::{AuthorizationKind, HttpConfig};
use crate::parser::Parser;
use crate::schema2;
use anyhow::Result;
use arrow::array::RecordBatch;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::NaiveDate;
use reqwest::{Client, Method};
use tracing::{error, info};

pub struct Http {
    config: HttpConfig,
    client: Client,
}

impl Http {
    fn new(config: HttpConfig) -> Result<Self> {
        Ok(Self {
            config,
            client: Client::builder().build()?,
        })
    }
    async fn get(&self, path: &str, query: &[QueryParam]) -> Result<String> {
        let url = reqwest::Url::parse(format!("{}/api", self.config.host.as_str()).as_str())?
            .join(path)?;
        let mut query = query.to_vec();
        match self.config.authorization_kind {
            AuthorizationKind::Header => (),
            AuthorizationKind::QueryParam => query.push(QueryParam::WsKey(self.config.key.clone())),
        };
        let query = render_query_params(&query);
        let builder = self.client.request(Method::GET, url);
        let builder = match self.config.authorization_kind {
            AuthorizationKind::Header => {
                let authorization_key = BASE64_STANDARD.encode(self.config.key.trim());
                let authorization_header =
                    "Basic".to_string() + " " + authorization_key.as_str() + ":";
                builder.header(reqwest::header::AUTHORIZATION, authorization_header)
            }
            AuthorizationKind::QueryParam => builder,
        };
        let builder = builder.query(&query);
        let request = builder.build()?;
        info!("url={}", request.url());
        info!("request={:?}", request);
        //.header(reqwest::header::AUTHORIZATION, authorization_header)
        // .query(&query)
        //.build()?;
        let resp = self.client.execute(request).await?;
        if !resp.status().is_success() {
            let msg = format!("HTTP status={} for url={}", resp.status(), resp.url());
            error!(msg);
            let body = resp.text().await?;
            error!("{}: <<EOF\n{}\nEOF\n", msg, body);
            return Err(anyhow::anyhow!(msg));
        }
        let s = resp.text().await?;
        Ok(s)
    }
}

pub async fn ws_get_available_resources(http: &Http) -> Result<Vec<Resource>> {
    //let url = format!("{}/api", WS_HOST);
    let response = http.get("/api", &[]).await?;
    let opt = roxmltree::ParsingOptions {
        ..roxmltree::ParsingOptions::default()
    };
    let doc = roxmltree::Document::parse_with_options(&response, opt)?;
    let p = Parser::new(doc.root_element());
    let r = p
        .named("prestashop")?
        .single_child()?
        .named("api")?
        .uniquely_named_children1()?
        .into_iter()
        .map(|c| Resource::new(c.node().tag_name().name().to_string()))
        .collect::<Vec<_>>();
    Ok(r)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Resource {
    identifier: String,
}
impl Resource {
    pub fn new(identifier: String) -> Self {
        Self { identifier }
    }
    pub fn identifier(&self) -> &str {
        self.identifier.as_str()
    }
}
pub mod query_param {
    #[derive(Clone)]
    pub enum Schema {
        Blank,
        Synopsis,
    }
    #[derive(Clone)]
    pub enum Display {
        Full,
        Fields(Vec<String>),
    }
}

#[derive(Clone)]
pub enum DateField {
    DateUpd,
    DateAdd,
}
impl DateField {
    pub fn identifier(&self) -> &str {
        match self {
            DateField::DateAdd => "date_add",
            DateField::DateUpd => "date_upd",
        }
    }
}

#[derive(Clone)]
pub enum QueryParam {
    Schema(query_param::Schema),
    Language(usize),
    Display(query_param::Display),
    Limit(usize),
    LimitFromIndex(usize, usize),
    WsKey(String),
    DateRange(DateField, NaiveDate, NaiveDate),
    FieldValueIn(String, Vec<String>),
}

fn render_query_params(params: &[QueryParam]) -> Vec<(String, String)> {
    let mut out = vec![];
    for p in params {
        match p {
            QueryParam::Limit(n) => out.push(("limit".to_string(), n.to_string())),
            QueryParam::LimitFromIndex(i, n) => {
                out.push(("limit".to_string(), format!("{},{}", i, n)))
            }
            QueryParam::FieldValueIn(field_name, values) => {
                let name = format!("filter[{}]", field_name);
                let value = format!("[{}]", values.join("|"));
                out.push((name, value))
            }
            QueryParam::Language(id) => out.push(("language".to_string(), id.to_string())),
            QueryParam::Schema(a) => out.push((
                "schema".to_string(),
                match a {
                    query_param::Schema::Blank => "blank",
                    query_param::Schema::Synopsis => "synopsis",
                }
                .to_string(),
            )),
            QueryParam::Display(query_param::Display::Full) => {
                out.push(("display".to_string(), "full".to_string()))
            }
            QueryParam::Display(query_param::Display::Fields(fields)) => {
                let fields = format!("[{}]", fields.join(","));
                out.push(("display".to_string(), fields));
            }
            QueryParam::WsKey(key) => out.push(("ws_key".to_string(), key.to_string())),
            QueryParam::DateRange(date_field, from, to) => {
                let value = format!("[{},{}]", from.format("%Y-%m-%d"), to.format("%Y-%m-%d"));
                out.push((format!("filter[{}]", date_field.identifier()), value));
                out.push(("date".to_string(), "1".to_string()));
            }
        }
    }
    out
}

pub async fn ws_get_resource_schema_string<'a>(
    http: &'a Http,
    resource: &'a Resource,
) -> Result<String> {
    let path = format!("/api/{}", resource.identifier());
    let response = http
        .get(
            path.as_str(),
            &[QueryParam::Schema(query_param::Schema::Synopsis)],
        )
        .await?;
    Ok(response)
}

pub async fn ws_get_resource_string(
    http: &Http,
    resource: &Resource,
    params: &[QueryParam],
) -> Result<String> {
    let path = format!("/api/{}", resource.identifier());
    //query.push(some(queryparam::language(1))); let response = http.get(&path, &query).await?;
    let response = http.get(&path, &params).await?;
    Ok(response)
}

pub async fn ws_get_resource_schema2(http: &Http, resource: &Resource) -> Result<schema2::Schema> {
    let response = &ws_get_resource_schema_string(http, resource).await?;
    let xml = roxmltree::Document::parse(response.as_str())?;
    let s = schema2::parse_schema(Parser::new(xml.root_element()))?;
    Ok(s)
}

pub async fn ws_get_resource_schema3(http: &Http, resource: &Resource) -> Result<schema3::Schema3> {
    let response = &ws_get_resource_schema_string(http, resource).await?;
    let schema = schema3::parse_schema(response.as_bytes())?;
    Ok(schema)
}

pub async fn ws_get_resource2(
    http: &Http,
    resource: &Resource,
    schema: &schema2::Schema,
    params: &[QueryParam],
) -> Result<serde_json::Value> {
    let response = &ws_get_resource_string(http, resource, params).await?;
    let doc = roxmltree::Document::parse(response)?;
    let json = schema2::parse_data_to_json(Parser::new(doc.root_element()), schema)?;
    Ok(json)
}

pub async fn ws_get_resource2_arrow(
    http: &Http,
    resource: &Resource,
    schema: &schema2::Schema,
    params: &[QueryParam],
) -> Result<RecordBatch> {
    let response = &ws_get_resource_string(http, resource, params).await?;
    let doc = roxmltree::Document::parse(response)?;
    let batch = schema2::parse_data_to_arrow(Parser::new(doc.root_element()), schema)?;
    Ok(batch)
}

pub async fn ws_get_resource2_arrow2(
    http: &Http,
    resource: &Resource,
    schema: &schema3::Schema3,
    params: &[QueryParam],
) -> Result<arrow2::chunk::Chunk<Box<dyn arrow2::array::Array>>> {
    let response = &ws_get_resource_string(http, resource, params).await?;
    let chunk = parse_response::parse_response_to_arrow(schema, response.as_bytes())?;
    Ok(chunk)
}

pub fn configure_http(conf_path: &str) -> Result<Http> {
    let conf: HttpConfig = toml::from_str(std::fs::read_to_string(conf_path)?.as_str())?;
    let http = Http::new(conf)?;
    Ok(http)
}
