use crate::parser::Parser;
use std::str::FromStr;

use anyhow::{anyhow, Result};

use serde_json::{Number, Value};

use arrow::datatypes::{DataType, Fields};

use crate::format::Format;

use std::sync::Arc;
use tracing::warn;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Record {
    fields: Vec<Field>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    record: Record,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Field {
    name: String,
    ty: Type,
}

impl Field {
    fn new(name: String, ty: Type) -> Self {
        Self { name, ty }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Type {
    Int32,
    UInt32,
    Float64,
    Utf8,
    Bool,
    Record(Record),
    List(Box<Field>),
    Language(u32),
}

impl Type {
    pub fn to_arrow(&self) -> DataType {
        match self {
            Type::Int32 => DataType::Int32,
            Type::UInt32 => DataType::UInt32,
            Type::Float64 => DataType::Float64,
            Type::Utf8 => DataType::Utf8,
            Type::Bool => DataType::Boolean,
            Type::Record(record) => DataType::Struct(
                record
                    .fields
                    .iter()
                    .map(|f| {
                        let name = f.name.to_string();
                        let ty = f.ty.to_arrow();
                        arrow::datatypes::Field::new(name, ty, true)
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Type::Language(_id) => DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    arrow::datatypes::Field::new("id", DataType::UInt32, true),
                    arrow::datatypes::Field::new("language", DataType::Utf8, true),
                ])),
                true,
            ))),
            Type::List(field) => DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Struct(
                    vec![arrow::datatypes::Field::new(
                        field.name.as_str(),
                        field.ty.to_arrow(),
                        true,
                    )]
                    .into(),
                ),
                true,
            ))),
        }
    }
}

impl Schema {
    pub fn to_arrow(&self) -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(
            self.record
                .fields
                .iter()
                .map(|f| arrow::datatypes::Field::new(f.name.to_string(), f.ty.to_arrow(), true))
                .collect::<Vec<_>>(),
        )
    }
}

mod pp {
    use super::*;
    pub fn pretty_print_type(ty: &Type, depth: usize, max_depth: usize) -> String {
        if depth < max_depth {
            match ty {
                Type::Record(rec) => pretty_print_record(rec, depth, max_depth),
                Type::List(field) => {
                    "[".to_string()
                        + format!(" <{}> : ", field.name).as_str()
                        + pretty_print_type(&field.ty, depth, max_depth).as_str()
                        + "]"
                }
                other => format!("{:?}", other),
            }
        } else {
            "...".to_string()
        }
    }

    pub fn pretty_print_record(record: &Record, depth: usize, max_depth: usize) -> String {
        let mut lines = vec![];
        let prefix = std::iter::repeat(' ').take(depth * 4).collect::<String>();
        let prefix2 = std::iter::repeat(' ')
            .take(depth * 4 + 4)
            .collect::<String>();
        lines.push("{".to_string());
        record.fields.iter().for_each(|field| {
            lines.push(format!(
                "{}{}: {}",
                prefix2,
                field.name,
                pretty_print_type(&field.ty, depth + 1, max_depth)
            ))
        });
        lines.push(format!("{}}}", prefix).to_string());
        lines.join("\n")
    }
}

pub fn pretty_print(schema: &Schema) -> String {
    pp::pretty_print_record(&schema.record, 0, usize::MAX)
}

pub fn pretty_print_max_depth(schema: &Schema, max_depth: usize) -> String {
    pp::pretty_print_record(&schema.record, 0, max_depth)
}

impl Type {
    fn from_name(name: &str) -> Option<Type> {
        if name.starts_with("id") || name.ends_with("id") {
            warn!(
                "assuming field '{}' is UInt32 because it has 'id' in the name",
                name
            );
            Some(Type::UInt32)
        } else {
            None
        }
    }
    fn from_format(f: &Format) -> Result<Type> {
        Ok(match f {
            Format::IsBool => Type::Bool,
            Format::IsUnsignedId => Type::UInt32,
            Format::IsUnsignedInt => Type::UInt32,
            Format::IsInt => Type::Int32,
            Format::IsUnsignedFloat => Type::Float64,
            Format::IsPrice => Type::Float64,

            // these are integers
            Format::IsEan13 => Type::Utf8,
            Format::IsUpc => Type::Utf8,
            Format::IsIsbn => Type::Utf8,
            //
            Format::IsDateFormat => Type::Utf8,
            Format::IsDate => Type::Utf8,
            //
            //^both|catalog|search|none$/i/
            Format::IsProductVisibility => Type::Utf8,
            //
            Format::IsString => Type::Utf8,
            Format::IsGenericName => Type::Utf8,
            Format::IsCatalogName => Type::Utf8,
            Format::IsCleanHtml => Type::Utf8,
            Format::IsLinkRewrite => Type::Utf8,
            Format::IsGenericName1 => Type::Utf8,
            Format::IsMpn => Type::Utf8,
            Format::IsReference => Type::Utf8,
            //_ => DataType::Utf8,
            _unsupported => return Err(anyhow!("format {:?} is not supported", f)),
        })
    }
}

fn try_type_from_format(p: &Parser) -> Result<Option<Type>> {
    if let Ok(format_string) = p.attribute("format") {
        let format = Format::from_string(format_string.to_string())?;
        if let Ok(ty) = Type::from_format(&format) {
            return Ok(Some(ty));
        }
    }
    Ok(None)
}

fn warp_in_record_field(field_name: String, struct_field_name: String, ty: Type) -> Field {
    Field::new(
        field_name,
        Type::Record(Record {
            fields: vec![Field::new(struct_field_name, ty)],
        }),
    )
}

pub fn parse_schema_field_type(name: Option<&str>, p: Parser) -> Result<Type> {
    let mut fields = vec![];
    let maybe_ty = try_type_from_format(&p)?.or_else(|| name.and_then(Type::from_name));

    if let Ok(v) = p.clone().only_same_named_children1() {
        let tmp = v[0].clone();
        if tmp.clone().named("language").is_ok()
            && !tmp.has_elements()
            && tmp.attribute("id").is_ok()
        {
            //return Ok(Type::MultilingualUtf8);
            let id = tmp.attribute("id")?.parse::<u32>()?;
            return Ok(Type::Language(id));
        }
    }

    if let Ok(_a) = p.clone().attribute("nodeType") {
        let p = p.single_child()?;
        let name = p.node().tag_name().name();
        let ty = parse_schema_field_type(Some(name), p)?;
        return Ok(Type::List(Box::new(Field {
            name: name.to_string(),
            ty,
        })));
    }

    for child in p.uniquely_named_children()? {
        let name = child.node().tag_name().name();
        let ty = parse_schema_field_type(Some(name), child)?;
        fields.push(Field {
            name: name.to_string(),
            ty,
        });
    }
    let ty = if fields.len() > 0 {
        Type::Record(Record { fields })
    } else if let Some(ty) = maybe_ty {
        ty
    } else {
        Type::Utf8
    };
    Ok(ty)
}

fn insert_id_field(mut schema: Schema) -> Result<Schema> {
    let id_field = Field {
        name: "id".to_string(),
        ty: Type::UInt32,
    };
    match &mut schema.record.fields[0].ty {
        Type::Record(ref mut record) => record.fields.insert(0, id_field),
        _ => return Err(anyhow!("failed inserting id field")),
    };
    Ok(schema)
}

fn insert_id_field2(mut record: Record) -> Result<Record> {
    let id_field = Field {
        name: "id".to_string(),
        ty: Type::UInt32,
    };
    record.fields.insert(0, id_field);
    Ok(record)
}
pub fn parse_schema(p: Parser) -> Result<Schema> {
    let ty = parse_schema_field_type(None, p)?;
    match ty {
        Type::Record(record) => Ok(insert_id_field(Schema { record })?),
        _ => Err(anyhow!(
            "schema must parse to struct, got this value:\n{:?}",
            ty
        )),
    }
}

fn parse_xml_list_field(p: Parser, field: &Field) -> Result<Value> {
    let value = parse_xml_node_to_json(p.named(field.name.as_str())?, &field.ty)?;
    Ok(Value::Object(serde_json::Map::from_iter(vec![(
        field.name.to_string(),
        value,
    )])))
}


fn parse_from_str<A: FromStr>(o: Parser) -> Result<Option<A>>
where
    <A as FromStr>::Err: std::fmt::Debug,
{
    let text = o.node().text().unwrap_or("").trim();
    if text.is_empty() {
        Ok(None)
    } else {
        let r = text.parse::<A>().map_err(|e| anyhow!("{:?}", e))?;
        Ok(Some(r))
    }
}

fn text_to_json_number<A: FromStr>(o: Parser) -> Result<Value>
where
    <A as FromStr>::Err: std::fmt::Debug,
    Number: From<A>,
{
    let text = o.node().text().unwrap_or("").trim();
    if text.is_empty() {
        Ok(Value::Null)
    } else {
        let r = text.parse::<A>().map_err(|e| anyhow!("{:?}", e))?;
        Ok(Value::Number(Number::from(r)))
    }
}

fn from_option(opt: Option<Value>) -> serde_json::Value {
    opt.unwrap_or(Value::Null)
}

fn parse_xml_node_to_json(p: Parser, ty: &Type) -> Result<serde_json::Value> {
    let r = match ty {
        Type::List(field) => {
            let v: Vec<_> = Result::from_iter(
                p.only_same_named_children()?
                    .into_iter()
                    .map(|c| parse_xml_list_field(c.clone(), &field)),
            )?;
            Value::Array(v)
        }
        Type::Language(_ty) => {
            let mut v = vec![];
            for c in p.only_same_named_children()? {
                let language = parse_xml_node_to_json(c.clone().named("language")?, &Type::Utf8)?;
                let id = c.attribute("id")?.parse::<u32>()?;
                let mut m = serde_json::Map::new();
                m.insert("language".to_string(), language);
                m.insert("id".to_string(), Value::Number(Number::from(id)));
                v.push(Value::Object(m));
            }
            Value::Array(v)
        }
        Type::Record(record) => parse_xml_record_to_json(p, &record)?,
        Type::Int32 => text_to_json_number::<i32>(p)?,
        Type::UInt32 => text_to_json_number::<u32>(p)?,
        Type::Float64 => {
            let opt: Option<f64> = parse_from_str(p)?;
            match opt {
                Some(x) => Value::Number(Number::from_f64(x).ok_or(anyhow!("failed parsing f64"))?),
                None => Value::Null,
            }
        }
        Type::Utf8 => from_option(p.node().text().map(|s| Value::String(s.to_string()))),
        Type::Bool => match p.node().text() {
            Some("1") => Value::Bool(true),
            Some("0") => Value::Bool(false),
            Some(z) => return Err(anyhow!("invalid boolean: '{}'", z)),
            None => Value::Null,
        },
    };
    Ok(r)
}

fn parse_xml_record_to_json(p: Parser, record: &Record) -> Result<serde_json::Value> {
    let elements = p.uniquely_named_children_map()?;
    let mut entries = serde_json::Map::new();
    for field in &record.fields {
        //ok_or(anyhow!("required field '{}' not found", field.name))?;
        if let Some(el) = elements.get(field.name.as_str()) {
            let json = parse_xml_node_to_json(el.clone(), &field.ty)?;
            entries.insert(field.name.to_string(), json);
        }
    }
    Ok(Value::Object(entries))
}

fn wrap_in_object(key: String, value: Value) -> Value {
    Value::Object(serde_json::Map::from_iter([(key, value)]))
}

#[tracing::instrument(skip(p, schema))]
pub fn parse_data_to_jsonl(p: Parser, schema: &Schema) -> Result<Vec<serde_json::Value>> {
    let ty = &schema.record.fields[0].ty;
    let mut out = vec![];
    for el in p.single_child()?.only_same_named_children()? {
        let name = el.node().tag_name().name().to_string();
        let json = parse_xml_node_to_json(el, &ty)?;
        let json = wrap_in_object(name, json);
        out.push(json);
    }
    Ok(out)
}

#[tracing::instrument(skip(p, schema))]
pub fn parse_data_to_json(p: Parser, schema: &Schema) -> Result<serde_json::Value> {
    let ty = Type::List(Box::new(schema.record.fields[0].clone()));
    parse_xml_node_to_json(p.single_child()?, &ty)
}

#[tracing::instrument(skip(p, schema))]
pub fn parse_data_to_arrow(p: Parser, schema: &Schema) -> Result<arrow::record_batch::RecordBatch> {
    let arrow_schema = Arc::new(schema.to_arrow());
    let mut decoder =
        arrow::json::reader::ReaderBuilder::new(arrow_schema.clone()).build_decoder()?;
    let json = parse_data_to_jsonl(p, schema)?;
    decoder.serialize(&json)?;
    let batch = decoder
        .flush()?
        .unwrap_or_else(|| arrow::record_batch::RecordBatch::new_empty(arrow_schema.clone()));
    Ok(batch)
}
