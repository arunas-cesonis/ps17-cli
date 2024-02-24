use anyhow::{anyhow, Result};
use arrow::datatypes::DataType;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Format {
    IsBool,
    IsFloat,
    IsInt,
    IsJson,
    IsNullOrUnsignedId,
    IsSerializedArray,
    IsString,
    IsUnsignedId,
    IsUnsignedInt,
    IsUnsignedFloat,
    IsAnything,
    IsApe,
    IsBirthDate,
    IsCleanHtml,
    IsColor,
    IsDate,
    IsDateFormat,
    IsEmail,
    IsImageSize,
    IsIp2Long,
    IsLanguageCode,
    IsLanguageIsoCode,
    IsLinkRewrite,
    IsLocale,
    IsMd5,
    IsNumericIsoCode,
    IsPasswd,
    IsPasswdAdmin,
    IsPercentage,
    IsPhpDateFormat,
    IsPriceDisplayMethod,
    IsReductionType,
    IsReference,
    IsSha1,
    IsThemeName,
    IsTrackingNumber,
    IsUrl,
    IsStockManagement,
    IsCatalogName,
    IsCarrierName,
    IsConfigName,
    IsCustomerName,
    IsGenericName,
    #[serde(rename = "IsGenericName")]
    IsGenericName1,
    IsImageTypeName,
    IsModuleName,
    IsName,
    IsTplName,
    IsAbsoluteUrl,
    IsEan13,
    IsIsbn,
    IsMpn,
    IsNegativePrice,
    IsPrice,
    IsProductVisibility,
    IsUpc,
    IsAddress,
    IsDniLite,
    IsCityName,
    IsCoordinate,
    IsMessage,
    IsPhoneNumber,
    IsPostCode,
    IsStateIsoCode,
    IsZipCodeFormat,
    IsDateOrNull,
}

impl Format {
    pub fn to_arrow(&self) -> Result<DataType> {
        Ok(match self {
            Format::IsBool => DataType::Boolean,
            Format::IsUnsignedId => DataType::UInt32,
            Format::IsUnsignedInt => DataType::UInt32,
            Format::IsInt => DataType::Int32,
            Format::IsUnsignedFloat => DataType::Float64,
            Format::IsPrice => DataType::Float64,

            // these are integers
            Format::IsEan13 => DataType::Utf8,
            Format::IsUpc => DataType::Utf8,
            Format::IsIsbn => DataType::Utf8,
            //
            Format::IsDateFormat => DataType::Utf8,
            Format::IsDate => DataType::Utf8,
            //
            //^both|catalog|search|none$/i/
            Format::IsProductVisibility => DataType::Utf8,
            //
            Format::IsString => DataType::Utf8,
            Format::IsGenericName => DataType::Utf8,
            Format::IsCatalogName => DataType::Utf8,
            Format::IsCleanHtml => DataType::Utf8,
            Format::IsLinkRewrite => DataType::Utf8,
            Format::IsGenericName1 => DataType::Utf8,
            Format::IsMpn => DataType::Utf8,
            Format::IsReference => DataType::Utf8,
            //_ => DataType::Utf8,
            _unsupported => return Err(anyhow!("format {:?} is not supported", self)),
        })
    }

    pub fn from_string(s: String) -> Result<Format> {
        let format: Format = serde_json::from_value(serde_json::Value::String(s))?;
        Ok(format)
    }
}
