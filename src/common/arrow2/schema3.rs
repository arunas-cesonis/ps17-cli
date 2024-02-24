use anyhow::{anyhow, Result};
use arrow2::datatypes::TimeUnit;
use serde_json;

use crate::format::Format;
use crate::arrow2::utils::{elements_of, parse_xml};

#[derive(Debug)]
pub struct Association {
    pub name: String,
    pub element_name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub struct Schema3 {
    pub fields: Vec<Field>,
    pub associations: Vec<Association>,
}

impl Schema3 {
    pub fn to_arrow2(&self) -> arrow2::datatypes::Schema {
        let mut fields = vec![];
        for f in &self.fields {
            fields.push(arrow2::datatypes::Field::new(
                &f.name,
                f.data_type.to_arrow2(),
                true,
            ));
        }
        let mut associations = vec![];
        for Association {
            name: assoc_name,
            element_name: _,
            fields: assoc_fields,
        } in &self.associations
        {
            let mut struct_fields = vec![];
            for f in assoc_fields {
                struct_fields.push(arrow2::datatypes::Field::new(
                    &f.name,
                    f.data_type.to_arrow2(),
                    true,
                ));
            }

            // associations
            let a = arrow2::datatypes::DataType::Struct(struct_fields);
            // associations.order_rows[]
            let c = arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                "item", a, true,
            )));
            let d = arrow2::datatypes::Field::new(assoc_name, c, true);
            associations.push(d);
        }
        if !associations.is_empty() {
            fields.push(arrow2::datatypes::Field::new(
                "associations",
                arrow2::datatypes::DataType::Struct(associations),
                true,
            ));
        }
        arrow2::datatypes::Schema::from(fields)
    }
}

#[derive(Debug)]
pub enum DataType {
    Int32,
    Date,
    Boolean,
    UInt32,
    Float64,
    Utf8,
    MultilingualUtf8,
}
impl DataType {
    pub fn to_arrow2(&self) -> arrow2::datatypes::DataType {
        match self {
            DataType::Utf8 => arrow2::datatypes::DataType::Utf8,
            DataType::Date => arrow2::datatypes::DataType::Timestamp(TimeUnit::Second, None),
            DataType::MultilingualUtf8 => {
                let item = arrow2::datatypes::DataType::Struct(vec![
                    arrow2::datatypes::Field::new("@id", arrow2::datatypes::DataType::UInt32, true),
                    arrow2::datatypes::Field::new("#text", arrow2::datatypes::DataType::Utf8, true),
                ]);
                arrow2::datatypes::DataType::List(Box::new(arrow2::datatypes::Field::new(
                    "item", item, true,
                )))
            }
            DataType::UInt32 => arrow2::datatypes::DataType::UInt32,
            DataType::Float64 => arrow2::datatypes::DataType::Float64,
            DataType::Int32 => arrow2::datatypes::DataType::Int32,
            DataType::Boolean => arrow2::datatypes::DataType::Boolean,
        }
    }
}

#[derive(Debug)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}
impl Field {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
        }
    }
    pub fn to_arrow2(&self) -> arrow2::datatypes::Field {
        arrow2::datatypes::Field::new(self.name.as_str(), self.data_type.to_arrow2(), true)
    }
}

fn type_from_name(name: &str) -> Option<DataType> {
    if name.starts_with("id") || name.ends_with("id") {
        Some(DataType::UInt32)
    } else if name.starts_with("date_") || name.ends_with("_date") {
        Some(DataType::Date)
    } else {
        None
    }
}

fn type_from_format(f: &Format) -> Result<DataType> {
    Ok(match f {
        Format::IsBool => DataType::Boolean,
        Format::IsUnsignedId => DataType::UInt32,
        Format::IsUnsignedInt => DataType::UInt32,
        Format::IsInt => DataType::Int32,
        Format::IsUnsignedFloat => DataType::Float64,
        Format::IsPrice => DataType::Float64,
        Format::IsDateFormat => DataType::Utf8,
        Format::IsDate => DataType::Date,
        _ => return Err(anyhow!("format {:?} is not supported", f)),
    })
}

fn parse_format_attribute(node: &roxmltree::Node) -> Result<Option<Format>> {
    if let Some(value) = node.attribute("format") {
        Ok(Some(serde_json::from_value(serde_json::Value::String(
            value.to_string(),
        ))?))
    } else {
        Ok(None)
    }
}
fn parse_simple_datatype(node: &roxmltree::Node) -> Result<DataType> {
    Ok(parse_format_attribute(node)?
        .and_then(|f| type_from_format(&f).ok())
        .or_else(|| type_from_name(node.tag_name().name()))
        .unwrap_or(DataType::Utf8))
}
fn has_language_child(node: &roxmltree::Node) -> bool {
    node.children()
        .any(|child| child.has_tag_name("language") && child.has_attribute("id"))
}

pub fn parse_schema(bytes: &[u8]) -> Result<Schema3> {
    let doc = parse_xml(bytes)?;
    let fields_container = doc
        .root_element()
        .first_element_child()
        .ok_or(anyhow!("no elements in root"))?;
    let mut fields = vec![Field {
        name: "id".to_string(),
        data_type: DataType::UInt32,
    }];
    let mut associations = vec![];
    for node in elements_of(&fields_container) {
        if node.has_tag_name("associations") {
            for assoc1 in elements_of(&node) {
                let assoc2 = assoc1
                    .first_element_child()
                    .ok_or(anyhow!("associations should have a child with fields"))?;
                let mut fields = vec![];
                for el in elements_of(&assoc2) {
                    fields.push(Field {
                        name: el.tag_name().name().to_string(),
                        data_type: parse_simple_datatype(&el)?,
                    });
                }
                associations.push(Association {
                    name: assoc1.tag_name().name().to_string(),
                    element_name: assoc2.tag_name().name().to_string(),
                    fields,
                });
            }
        } else if has_language_child(&node) {
            fields.push(Field {
                name: node.tag_name().name().to_string(),
                data_type: DataType::MultilingualUtf8,
            });
        } else {
            fields.push(Field {
                name: node.tag_name().name().to_string(),
                data_type: parse_simple_datatype(&node)?,
            });
        };
    }
    Ok(Schema3 {
        fields,
        associations
    })
}
