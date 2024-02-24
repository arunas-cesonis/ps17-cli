extern crate core;

use std::ops::Sub;

use ::tracing::level_filters::LevelFilter;
use anyhow::{anyhow, Result};
use arrow::array::{Array, StructArray};
use arrow::record_batch::RecordBatch;

use common::http::{
    configure_http, query_param, ws_get_available_resources, ws_get_resource2_arrow,
    ws_get_resource2_arrow2, ws_get_resource_schema2, ws_get_resource_schema3, DateField, Http,
    QueryParam, Resource,
};

use crate::arguments::{Arguments, Command, Limit, OutputFormat};
use crate::output::{OutputFile, OutputStdout, OutputT};

mod arguments;
mod output;

use common::utils;
fn flatten_single_toplevel_struct(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_columns() != 1 {
        return Err(anyhow!(
            "cannot flatten1 when top level has more than one field"
        ));
    }
    let sa = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow!("failed casting ot StructArray"))?;
    let new_batch = RecordBatch::from(sa);
    Ok(new_batch)
}

pub async fn run_command<W, O>(args: Arguments, _http: Http, output: O) -> Result<()>
where
    W: std::io::Write + Send,
    O: OutputT<W>,
{
    match args.command {
        Command::GetAvailableResources(args) => {
            let http = configure_http(args.conf.as_str())?;
            let r = ws_get_available_resources(&http).await?;
            output.json(std::iter::once(r))?;
        }
        Command::GetSchema(args) => {
            let http = configure_http(args.common.conf.as_str())?;
            let r = ws_get_resource_schema2(&http, &Resource::new(args.resource)).await?;
            output.json(std::iter::once(r))?;
        }
        Command::Get(args) => {
            let http = configure_http(args.common.conf.as_str())?;
            let mut params = vec![];
            match args.limit.unwrap_or_default() {
                Limit::All => (),
                Limit::Limit(n) => params.push(QueryParam::Limit(n)),
                Limit::LimitFromIndex(i, n) => params.push(QueryParam::LimitFromIndex(i, n)),
            }
            if let Some(arguments::DateRange { from, to }) = args.date_add {
                params.push(QueryParam::DateRange(DateField::DateAdd, from, to));
            }
            if let Some(arguments::DateRange { from, to }) = args.date_upd {
                params.push(QueryParam::DateRange(DateField::DateUpd, from, to));
            }
            params.push(if let Some(fields) = args.fields {
                QueryParam::Display(query_param::Display::Fields(fields))
            } else {
                QueryParam::Display(query_param::Display::Full)
            });

            if let Some(fvi) = args.field_value_in {
                params.push(QueryParam::FieldValueIn(fvi.field_name, fvi.values));
            }
            let _from = chrono::Utc::now().sub(chrono::Duration::days(60));
            let _to = chrono::Utc::now();
            //
            // let from = SystemTime::now().sub(Duration::
            let res = Resource::new(args.resource.clone());
            if args.arrow2 {
                let s = ws_get_resource_schema3(&http, &res).await?;
                let r = ws_get_resource2_arrow2(&http, &res, &s, &params).await?;
                match args.output_format_args.output_format.unwrap_or_default() {
                    OutputFormat::JSON => {
                        output.json2(s.to_arrow2(), std::iter::once(r))?;
                    }
                    OutputFormat::Parquet => {
                        output.parquet2(s.to_arrow2(), std::iter::once(r))?;
                    }
                };
            } else {
                let s = ws_get_resource_schema2(&http, &res).await?;
                let r = ws_get_resource2_arrow(&http, &res, &s, &params).await?;
                let r = if args.flatten1 {
                    flatten_single_toplevel_struct(&r)?
                } else {
                    r
                };
                match args.output_format_args.output_format.unwrap_or_default() {
                    OutputFormat::JSON => {
                        output.arrow_json(std::iter::once(r))?;
                    }
                    OutputFormat::Parquet => {
                        output.parquet(std::iter::once(r))?;
                    }
                };
            }
        }
    };
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    utils::setup_tracing(LevelFilter::TRACE);
    let args = Arguments::parse();
    let http = configure_http(args.get_common().conf.as_str())?;
    if let Some(output_path) = args.get_output_path() {
        let output = OutputFile::new(output_path);
        run_command(args, http, output).await?;
    } else {
        let output = OutputStdout::new();
        run_command(args, http, output).await?;
    }
    Ok(())
}
