use anyhow::anyhow;
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(ValueEnum, Clone)]
pub enum OutputFormat {
    JSON,
    Parquet,
}
impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::JSON
    }
}
#[derive(Parser)]
pub struct OutputFormatArgs {
    #[arg(long, required = false)]
    pub output_format: Option<OutputFormat>,
}
#[derive(Parser)]
pub struct Common {
    #[arg(long, required = true)]
    pub conf: String,

    #[arg(long, required = false)]
    pub output_path: Option<PathBuf>,
}
#[derive(Parser)]
pub struct GetSchema {
    #[arg(required = true)]
    pub resource: String,

    #[command(flatten)]
    pub common: Common,
}

#[derive(Clone, Debug)]
pub struct DateRange {
    pub from: NaiveDate,
    pub to: NaiveDate,
}
impl FromStr for DateRange {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((from, to)) = s.split_once("..") {
            let from = NaiveDate::from_str(from)?;
            let to = NaiveDate::from_str(to)?;
            Ok(DateRange { from, to })
        } else {
            Err(anyhow!(
                "expected date range in format: 2020-10-10..2021-10-10"
            ))
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldValueIn {
    pub field_name: String,
    pub values: Vec<String>,
}

// use JSON here?
impl FromStr for FieldValueIn {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let error = Err(anyhow!("expected format is 'field_name=value1|value2|..'"));
        //let mut s = s.chars().collect::<Vec<_>>();
        let (field_name, s) = match s.split_once('=') {
            Some(a) => a,
            None => return error,
        };
        if field_name.is_empty() {
            return error;
        }
        let field_name = field_name.to_string();
        let mut values = vec![];
        let mut input = s.chars();
        let mut tmp = vec![];
        while let Some(c) = input.next() {
            match c {
                '\\' => {
                    tmp.push(
                        input
                            .next()
                            .ok_or(anyhow!("unexpected end of input after '\\'"))?,
                    );
                }
                '|' => {
                    values.push(String::from_iter(&tmp));
                    tmp.clear();
                }
                _ => {
                    tmp.push(c);
                }
            }
        }
        if !tmp.is_empty() {
            values.push(String::from_iter(&tmp));
        }
        if values.is_empty() {
            return error;
        }
        Ok(FieldValueIn {
            field_name: field_name.to_string(),
            values,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let x = <FieldValueIn as FromStr>::from_str("field=ab\\|c|de|f||g").unwrap();
        assert_eq!(x.field_name.as_str(), "field");
        assert_eq!(x.values, vec!["ab|c", "de", "f", "", "g"]);
        let x = <FieldValueIn as FromStr>::from_str("field=a").unwrap();
        assert_eq!(x.field_name.as_str(), "field");
        assert_eq!(x.values, vec!["a"]);
        assert!(<FieldValueIn as FromStr>::from_str("=a").is_err());
        assert!(<FieldValueIn as FromStr>::from_str("a=").is_err());
    }
}

#[derive(Clone, Debug)]
pub enum Limit {
    All,
    Limit(usize),
    LimitFromIndex(usize, usize),
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Limit as Debug>::fmt(self, f)
    }
}

impl Default for Limit {
    fn default() -> Self {
        Limit::All
    }
}

impl FromStr for Limit {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "all" {
            Ok(Limit::All)
        } else if let Some((a, b)) = s.split_once(",") {
            let a = a.parse::<usize>()?;
            let b = b.parse::<usize>()?;
            Ok(Limit::LimitFromIndex(a, b))
        } else {
            Ok(Limit::Limit(s.parse::<usize>()?))
        }
    }
}

#[derive(Parser)]
pub struct Get {
    #[arg(required = true)]
    pub resource: String,

    /// Supported formats are 'all', '10' and '10,20' where 'all' disables
    /// limiting, '10' limits to 10 records and '10,20' limits to 10 records from index 10
    #[arg(short, long, required = false)]
    pub limit: Option<Limit>,

    #[arg(short, long, required = false, value_name = "field")]
    pub fields: Option<Vec<String>>,

    #[arg(long, required = false)]
    pub field_value_in: Option<FieldValueIn>,

    #[command(flatten)]
    pub common: Common,

    #[command(flatten)]
    pub output_format_args: OutputFormatArgs,

    /// Date range passed as filter on date_upd field. Argument format: 2020-10-10..2021-10-10
    #[arg(long, required = false)]
    pub date_upd: Option<DateRange>,

    /// Date range passed as filter on date_add field. Argument format: 2020-10-10..2021-10-10
    #[arg(long, required = false)]
    pub date_add: Option<DateRange>,

    /// Flattens first level of nested structs so that fields of the resource    
    /// are at the top level
    #[arg(long, required = false, default_value_t = false)]
    pub flatten1: bool,

    /// Use arrow2 instead of arrow1 where implemented
    /// This always means --flatten1 too
    #[arg(long, required = false, default_value_t = false)]
    pub arrow2: bool,
}

#[derive(Subcommand)]
pub enum Command {
    Get(Get),
    GetSchema(GetSchema),
    GetAvailableResources(Common),
}

#[derive(Parser)]
pub struct Arguments {
    #[command(subcommand)]
    pub command: Command,
}

impl Arguments {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
    pub fn get_output_path(&self) -> &Option<PathBuf> {
        &self.get_common().output_path
    }
    pub fn get_common(&self) -> &Common {
        match self.command {
            Command::Get(ref args) => &args.common,
            Command::GetSchema(ref args) => &args.common,
            Command::GetAvailableResources(ref args) => &args,
        }
    }
    pub fn get_output_format(&self) -> &Option<OutputFormat> {
        match self.command {
            Command::Get(ref args) => &args.output_format_args.output_format,
            Command::GetSchema(ref _args) => &None,
            Command::GetAvailableResources(ref _args) => &None,
        }
    }
}
