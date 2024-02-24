use arrow2::array::{Array, StructArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;


pub fn write_ndjson<W, I>(writer:W, array: I) where W: std::io::Write, I : IntoIterator<Item = Box<dyn Array>>{
    let serializer =
        arrow2::io::ndjson::write::Serializer::new(array.into_iter().map(Ok), vec![]);

    let mut writer = arrow2::io::ndjson::write::FileWriter::new(writer, serializer);
    writer.by_ref().for_each(|x| x.unwrap());
}

pub fn chunk_to_array(
    schema: &arrow2::datatypes::Schema,
    chunk: Chunk<Box<dyn Array>>,
) -> Box<dyn Array> {
    StructArray::new(
        DataType::Struct(schema.fields.clone()),
        chunk.into_arrays(),
        None,
    )
    .boxed()
}

pub fn parse_xml(bytes: &[u8]) -> anyhow::Result<roxmltree::Document> {
    let doc = roxmltree::Document::parse(simdutf8::basic::from_utf8(bytes)?)?;
    Ok(doc)
}

pub fn elements_of<'a>(
    node: &'a roxmltree::Node<'a, 'a>,
) -> impl Iterator<Item = roxmltree::Node<'a, 'a>> {
    node.children().filter(|c| c.is_element())
}

pub fn format_schema_compact(schema: &arrow2::datatypes::Schema) -> String {
    let mut stack: Vec<_> = schema.fields.iter().cloned().map(|f| (0, f)).collect();
    let mut lines = vec![];
    while let Some((d, x)) = stack.pop() {
        let _prefix = "    ".repeat(d);
        let ty = match x.data_type {
            DataType::Struct(fields) => {
                stack.extend(fields.into_iter().map(|f| (d + 1, f)));
                "struct"
            }
            DataType::List(field) => {
                stack.push((d + 1, *field.clone()));
                "list"
            }
            DataType::Utf8 => "string",
            DataType::UInt32 => "uint32",
            DataType::Int32 => "int32",
            DataType::Float64 => "float64",
            DataType::Date64 => "date64",
            _ => "unknown",
        };
        lines.push(format!("lvl={} {}: {}", d, x.name, ty));
    }
    lines.join("\n")
}
