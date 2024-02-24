use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType as Arrow2DataType, TimeUnit};
use arrow2::datatypes::Field;
use arrow2::types::{NativeType, Offset};
use chrono::NaiveDateTime;

use crate::arrow2::schema3;
use crate::arrow2::schema3::{Association, DataType, Schema3};
use crate::arrow2::utils::{elements_of, parse_xml};

fn to_box<M>(m: M) -> Box<dyn MutableArray>
where
    M: MutableArray + 'static,
{
    Box::new(m) as Box<dyn MutableArray>
}

fn association_to_mutable_array(association: &Association) -> Result<Box<dyn MutableArray>> {
    let data_type = Arrow2DataType::Struct(
        association
            .fields
            .iter()
            .map(schema3::Field::to_arrow2)
            .collect(),
    );
    let arrays = Result::from_iter(
        association
            .fields
            .iter()
            .map(|f| data_type_to_mutable_array(&f.data_type)),
    )?;
    let obj = MutableStructArray::new(data_type, arrays);
    let list: MutableListArray<i32, Box<dyn MutableArray>> =
        MutableListArray::new_with_capacity(to_box(obj), 16);
    Ok(to_box(list))
}

fn associations_to_mutable_array(associations: &[Association]) -> Result<Box<dyn MutableArray>> {
    let arrays: Vec<_> =
        Result::from_iter(associations.iter().map(|a| association_to_mutable_array(a)))?;
    let fields = associations
        .iter()
        .map(|a| a.name.as_str())
        .zip(arrays.iter().map(|a| a.data_type().clone()))
        .map(|(name, data_type)| Field::new(name, data_type, true));
    let data_type = Arrow2DataType::Struct(fields.collect());
    let array = MutableStructArray::new(data_type, arrays);
    Ok(to_box(array))
}

fn data_type_to_mutable_array(data_type: &DataType) -> Result<Box<dyn MutableArray>> {
    Ok(match &data_type {
        DataType::Int32 => to_box(MutablePrimitiveArray::<i32>::new()),
        DataType::UInt32 => to_box(MutablePrimitiveArray::<u32>::new()),
        DataType::Float64 => to_box(MutablePrimitiveArray::<f64>::new()),
        DataType::Date => to_box(MutablePrimitiveArray::<i64>::try_new(
            arrow2::datatypes::DataType::Timestamp(TimeUnit::Second, None),
            vec![],
            None,
        )?),
        DataType::Utf8 => to_box(MutableUtf8Array::<i32>::new()),
        DataType::Boolean => to_box(MutableBooleanArray::new()),
        DataType::MultilingualUtf8 => {
            let language = MutableUtf8Array::<i32>::new();
            let id = MutablePrimitiveArray::<u32>::new();
            let obj = MutableStructArray::new(
                arrow2::datatypes::DataType::Struct(vec![
                    Field::new("@id", arrow2::datatypes::DataType::UInt32, true),
                    Field::new("#text", arrow2::datatypes::DataType::Utf8, true),
                ]),
                vec![to_box(id), to_box(language)],
            );
            let items = to_box(obj);
            let list: MutableListArray<i32, Box<dyn MutableArray>> =
                MutableListArray::new_with_capacity(items, 4);
            to_box(list)
        }
    })
}

fn downcast<T: MutableArray + 'static>(dst: &mut Box<dyn MutableArray>) -> Result<&mut T> {
    let error_info = format!("{:?}", dst.data_type());
    let dst = dst.as_mut_any().downcast_mut::<T>().ok_or_else(|| {
        anyhow!(
            "downcast {} to {} failed",
            error_info,
            std::any::type_name::<T>()
        )
    })?;
    Ok(dst)
}

fn parse_utf8<O: Offset>(dst: &mut Box<dyn MutableArray>, src: Option<&str>) -> Result<()> {
    let dst = downcast::<MutableUtf8Array<O>>(dst)?;
    if let Some(s) = src {
        dst.try_push(Some(s))?;
    } else {
        dst.push_null();
    }
    Ok(())
}

fn parse_bool(dst: &mut Box<dyn MutableArray>, src: Option<&str>) -> Result<()> {
    let dst = downcast::<MutableBooleanArray>(dst)?;
    match src {
        Some("1") => dst.try_push(Some(true))?,
        Some("0") => dst.try_push(Some(false))?,
        None => dst.push_null(),
        Some(other) => return Err(anyhow!("invalid bool value {}", other)),
    };
    Ok(())
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    match s.map(|s| s.trim()) {
        Some("") => None,
        None => None,
        Some(x) => Some(x),
    }
}

fn parse_from_str<A: FromStr + NativeType>(
    dst: &mut Box<dyn MutableArray>,
    src: Option<&str>,
) -> Result<()>
where
    <A as FromStr>::Err: std::error::Error + Sync + Send + 'static,
{
    let dst = downcast::<MutablePrimitiveArray<A>>(dst)?;
    if let Some(s) = non_empty(src) {
        dst.try_push(Some(s.parse::<A>()?))?;
    } else {
        dst.push_null();
    }
    Ok(())
}

fn parse_field_from_str<A: FromStr + NativeType>(
    dst: &mut Box<dyn MutableArray>,
    src: &roxmltree::Node,
) -> Result<()>
where
    <A as FromStr>::Err: std::error::Error + Sync + Send + 'static,
{
    parse_from_str::<A>(dst, src.text())
}

fn parse_f64(dst: &mut Box<dyn MutableArray>, src: Option<&str>) -> Result<()> {
    let dst = downcast::<MutablePrimitiveArray<f64>>(dst)?;
    if let Some(s) = src {
        dst.try_push(Some(s.parse::<f64>()?))?;
    } else {
        dst.push_null();
    }
    Ok(())
}

fn parse_u32(dst: &mut Box<dyn MutableArray>, src: Option<&str>) -> Result<()> {
    let dst = downcast::<MutablePrimitiveArray<u32>>(dst)?;
    if let Some(s) = src {
        dst.try_push(Some(s.parse::<u32>()?))?;
    } else {
        dst.push_null();
    }
    Ok(())
}

fn parse_field_utf8<O: Offset>(
    dst: &mut Box<dyn MutableArray>,
    src: &roxmltree::Node,
) -> Result<()> {
    parse_utf8::<O>(dst, src.text())
}

fn parse_field_date64(dst: &mut Box<dyn MutableArray>, src: &roxmltree::Node) -> Result<()> {
    assert_eq!(
        dst.data_type(),
        &Arrow2DataType::Timestamp(TimeUnit::Second, None)
    );
    let dst = downcast::<MutablePrimitiveArray<i64>>(dst)?;
    if let Some(s) = src.text() {
        let date = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?;
        dst.try_push(Some(date.timestamp_millis()))?;
    } else {
        dst.push_null();
    }
    Ok(())
}

fn parse_field_f64(dst: &mut Box<dyn MutableArray>, src: &roxmltree::Node) -> Result<()> {
    parse_f64(dst, src.text())
}

fn parse_field_bool(dst: &mut Box<dyn MutableArray>, src: &roxmltree::Node) -> Result<()> {
    parse_bool(dst, src.text())
}

fn parse_field_u32(dst: &mut Box<dyn MutableArray>, src: &roxmltree::Node) -> Result<()> {
    parse_u32(dst, src.text())
}

fn parse_field_list<O: Offset>(
    dst: &mut Box<dyn MutableArray>,
    src: &roxmltree::Node,
) -> Result<()> {
    let dst = downcast::<MutableListArray<O, Box<dyn MutableArray>>>(dst)?;
    let values = dst.mut_values();
    for el in elements_of(src) {
        parse_field(values, &el)?;
    }
    dst.try_push_valid()?;
    Ok(())
}

fn parse_field_struct<O: Offset>(
    dst: &mut Box<dyn MutableArray>,
    src: &roxmltree::Node,
) -> Result<()> {
    let dst = downcast::<MutableStructArray>(dst)?;
    let data_type = dst.data_type().clone();
    let fields = match &data_type {
        Arrow2DataType::Struct(fields) => fields,
        _ => return Err(anyhow!("expected struct")),
    };
    let _tag_name = src.tag_name().name();
    let mut parsed_any = false;
    let initial_len = dst.len();

    // XML specific parsing similar to https://pypi.org/project/xmltodict/
    // to support <language> tags 'generically'
    for (i, field) in fields.iter().enumerate() {
        if &field.name[0..1] == "@" && field.data_type == Arrow2DataType::UInt32 {
            let attribute_name = &field.name[1..field.name.len()];
            parse_u32(&mut dst.mut_values()[i], src.attribute(attribute_name))?;
            parsed_any = true;
        } else if field.name == "#text" && field.data_type == Arrow2DataType::Utf8 {
            parse_utf8::<i32>(&mut dst.mut_values()[i], src.text())?;
            parsed_any = true;
        }
    }
    for el in elements_of(src) {
        let field_name = el.tag_name().name();
        let field_index = fields
            .iter()
            .position(|x| x.name == field_name)
            .ok_or_else(|| anyhow!("unknown field {}", field_name))?;
        parse_field(&mut dst.mut_values()[field_index], &el)?;
        parsed_any = true;
    }
    if parsed_any {
        for i in 0..dst.mut_values().len() {
            if dst.mut_values()[i].len() != initial_len + 1 {
                assert_eq!(dst.mut_values()[i].len(), initial_len);
                dst.mut_values()[i].push_null();
            }
        }
    }
    dst.push(true);
    Ok(())
}

fn parse_field(dst: &mut Box<dyn MutableArray>, src: &roxmltree::Node) -> Result<()> {
    match dst.data_type() {
        Arrow2DataType::Utf8 => parse_field_utf8::<i32>(dst, src).context("parse_field_utf8"),
        Arrow2DataType::UInt32 => parse_field_from_str::<u32>(dst, src).context("parse_field_u32"),
        Arrow2DataType::Int32 => parse_field_from_str::<i32>(dst, src).context("parse_field_i32"),
        Arrow2DataType::Float64 => parse_field_from_str::<f64>(dst, src).context("parse_field_f64"),
        Arrow2DataType::Timestamp(TimeUnit::Second, None) => {
            parse_field_date64(dst, src).context("parse_field_ts")
        }
        Arrow2DataType::Boolean => parse_field_bool(dst, src).context("parse_field_bool"),
        Arrow2DataType::List(_) => parse_field_list::<i32>(dst, src)
            .with_context(|| format!("parse_field_list {:?}", src.tag_name().name())),
        Arrow2DataType::Struct(_) => parse_field_struct::<i32>(dst, src)
            .with_context(|| format!("parse_field_struct {:?}", src.tag_name().name())),
        other => return Err(anyhow!("arrow parsing for {:?} is not implemented", other)),
    }
}

pub fn parse_response_to_arrow(schema: &Schema3, bytes: &[u8]) -> Result<Chunk<Box<dyn Array>>> {
    let doc = parse_xml(bytes)?;
    let container = doc
        .root_element()
        .first_element_child()
        .ok_or(anyhow!("no elements in root"))?;

    let mut h = HashMap::new();
    for (i, f) in schema.fields.iter().enumerate() {
        let mutable_array = data_type_to_mutable_array(&f.data_type)?;
        h.insert(f.name.to_string(), (i, mutable_array));
    }
    if !schema.associations.is_empty() {
        h.insert(
            "associations".to_string(),
            (
                h.len(),
                associations_to_mutable_array(&schema.associations)?,
            ),
        );
    }
    for (i, el) in elements_of(&container).enumerate() {
        for field in elements_of(&el) {
            let field_name = field.tag_name().name();
            let (_, ref mut array) = h
                .get_mut(field_name)
                .ok_or_else(|| anyhow!("unknown field {}", field_name))?;
            parse_field(array, &field)
                .with_context(|| format!("parse_field {:?}", el.tag_name().name()))?;
        }
        for (_, ref mut array) in h.values_mut() {
            if array.len() == i {
                array.push_null();
            } else {
                assert_eq!(array.len(), i + 1);
            }
        }
    }
    let num_fields = schema.fields.len()
        + if !schema.associations.is_empty() {
            1
        } else {
            0
        };
    let mut arrays: Vec<Option<Box<dyn Array>>> = vec![None; num_fields];
    for (i, mut array) in h.into_values() {
        arrays[i] = Some(array.as_box());
    }
    let arrays = arrays.into_iter().filter_map(|x| x).collect::<Vec<_>>();
    Ok(Chunk::new(arrays))
}

#[cfg(test)]
mod test {
    use arrow2::array::Utf8Array;

    use crate::arrow2::parse_response::parse_response_to_arrow;
    use crate::arrow2::schema3::{Association, DataType, Field, Schema3};

    #[test]
    fn test_parse_simple_response() {
        let schema = Schema3 {
            fields: vec![Field {
                name: "name".to_string(),
                data_type: DataType::Utf8
            }],
            associations: vec![],
        };
        let source = r#"
        <toplevel>
            <elements>
                <element>
                    <name>a</name>
                </element>
                <element>
                </element>
                <element>
                    <name>c</name>
                </element>
            </elements>
        </toplevel>
        "#;

        let result = parse_response_to_arrow(&schema, source.as_bytes()).unwrap();
        let vec = result.arrays()[0]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(vec, vec![Some("a"), None, Some("c")]);
    }

    #[test]
    fn test_parse_multilingual_field() {
        let schema = Schema3 {
            fields: vec![Field {
                name: "name".to_string(),
                data_type: DataType::MultilingualUtf8,
            }],
            associations: vec![],
        };
        let source = r#"
        <toplevel>
            <elements>
                <element>
                    <name>
                        <language id="1">a</language>
                        <language id="2">b</language>
                    </name>
                </element>
                <element>
                </element>
                <element>
                    <name>
                        <language id="1">c</language>
                        <language id="2">d</language>
                    </name>
                </element>
            </elements>
        </toplevel>
        "#;

        let _result = parse_response_to_arrow(&schema, source.as_bytes()).unwrap();
        //assert_eq!(vec, vec![Some("a"), None, Some("c")]);
    }

    #[test]
    fn test_parse_associations() {
        let schema = Schema3 {
            fields: vec![Field {
                name: "name".to_string(),
                data_type: DataType::MultilingualUtf8,
            }],
            associations: vec![Association {
                name: "categories".to_string(),
                element_name: "category".to_string(),
                fields: vec![
                    Field {
                        name: "id".to_string(),
                        data_type: DataType::UInt32,
                    },
                    //[Field {
                    //[    name: "name".to_string(),
                    //[    data_type: DataType::MultilingualUtf8
                    //[}
                ],
            }],
        };
        let source = r#"
        <toplevel>
            <elements>
                <element>
                    <name>
                        <language id="1">a</language>
                        <language id="2">b</language>
                    </name>
                    <associations>
                        <categories>
                            <category><id>1</id></category>
                            <category><id>2</id></category>
                            <category><id>3</id></category>
                        </categories>
                    </associations>
                </element>
                <element>
                </element>
                <element>
                    <name>
                        <language id="3">c</language>
                        <language id="4">d</language>
                    </name>
                    <associations>
                        <categories>
                            <category><id>1</id></category>
                            <category><id>3</id></category>
                        </categories>
                    </associations>
                </element>
            </elements>
        </toplevel>
        "#;

        let result = parse_response_to_arrow(&schema, source.as_bytes()).unwrap();
        eprintln!("{:#?}", result);
        //assert_eq!(vec, vec![Some("a"), None, Some("c")]);
    }
}
