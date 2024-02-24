use anyhow::Result;

use arrow::record_batch::RecordBatch;

use arrow2::chunk::Chunk;
use arrow2::io::parquet::write::{transverse, FileWriter, RowGroupIterator, WriteOptions};
use common::arrow2::utils::{chunk_to_array, write_ndjson};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::write::Version;
use std::io::Stdout;
use std::path::{Path, PathBuf};
use tracing::info;

pub trait OutputT<W>
where
    W: std::io::Write + Send,
{
    fn to_writer(&self) -> Result<W>;

    #[tracing::instrument(skip(self, iter))]
    fn json2<I>(self, schema: arrow2::datatypes::Schema, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Chunk<Box<dyn arrow2::array::Array>>>,
        Self: Sized,
    {
        let iter = iter.into_iter().map(|chunk| chunk_to_array(&schema, chunk));
        write_ndjson(self.to_writer()?, iter);
        Ok(())
    }

    #[tracing::instrument(skip(self, iter))]
    fn parquet<I>(self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = RecordBatch>,
        Self: Sized,
    {
        let mut iter = iter.into_iter();
        let first = if let Some(batch) = iter.next() {
            batch
        } else {
            return Ok(());
        };
        let mut writer = self.to_writer()?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut writer, first.schema(), None)?;
        let mut total = first.num_rows();
        writer.write(&first)?;
        for other in iter {
            total += other.num_rows();
            writer.write(&other)?;
        }
        info!("wrote {} rows", total);
        writer.close()?;
        Ok(())
    }
    #[tracing::instrument(skip(self, iter))]
    fn parquet2<I>(self, schema: arrow2::datatypes::Schema, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Chunk<Box<dyn arrow2::array::Array>>>,
        Self: Sized,
    {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(
            iter.into_iter().map(|x| Ok(x)),
            &schema,
            options,
            encodings,
        )?;
        let file = self.to_writer()?;
        let mut writer = FileWriter::try_new(file, schema, options)?;
        for group in row_groups {
            writer.write(group?)?;
        }
        let sz = writer.end(None)?;
        info!("wrote {} bytes", sz);
        Ok(())
    }
    #[tracing::instrument(skip(self, iter))]
    fn arrow_json<I>(self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = RecordBatch>,
        Self: Sized,
    {
        let mut iter = iter.into_iter();
        let first = if let Some(batch) = iter.next() {
            batch
        } else {
            return Ok(());
        };
        let writer = self.to_writer()?;
        let mut writer = arrow::json::LineDelimitedWriter::new(writer);
        let mut total = first.num_rows();
        writer.write(&first)?;
        for other in iter {
            total += other.num_rows();
            writer.write(&other)?;
        }
        info!("wrote {} rows", total);
        writer.finish()?;
        Ok(())
    }

    #[tracing::instrument(skip(self, iter))]
    fn json<I, A>(self, iter: I) -> Result<()>
    where
        A: serde::Serialize,
        I: IntoIterator<Item = A>,
        Self: Sized,
    {
        let mut writer = self.to_writer()?;
        let mut total = 0;
        for a in iter {
            serde_json::to_writer(&mut writer, &a)?;
            writer.write(b"\n")?;
            total += 1;
        }
        info!("wrote {} rows", total);
        Ok(())
    }
}

pub struct OutputFile {
    path: PathBuf,
}
impl OutputFile {
    pub fn new<A: AsRef<Path>>(path: A) -> Self {
        OutputFile {
            path: path.as_ref().to_path_buf(),
        }
    }
}
impl OutputT<std::fs::File> for OutputFile {
    fn to_writer(&self) -> Result<std::fs::File> {
        Ok(std::fs::File::create(&self.path)?)
    }
}
impl OutputStdout {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct OutputStdout {}
impl OutputT<Stdout> for OutputStdout {
    fn to_writer(&self) -> Result<Stdout> {
        Ok(std::io::stdout())
    }
}
