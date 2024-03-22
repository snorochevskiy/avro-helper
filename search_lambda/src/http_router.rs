
use crate::lambda_env;
use crate::s3_util;
use crate::avro_filter;

use std::collections::HashMap;

use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema, Writer};

use bytes::Buf;
use lambda_runtime::Error;
use serde::{Deserialize, Serialize};

use base64::prelude::*;

use tracing::debug;

// Format spec. https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html
#[derive(Debug,Deserialize)]
pub struct Request {
    pub version: String,
    pub rawPath: String,
    pub rawQueryString: String,
    pub headers: HashMap<String,String>,
    pub queryStringParameters: HashMap<String,String>,
    pub requestContext: RequestContext,
    pub body: Option<String>,
}

#[derive(Debug,Deserialize)]
pub struct RequestContext {
    pub http: HttpInfo,
}

#[derive(Debug,Deserialize)]
pub struct HttpInfo {
    pub method: String,
    pub path: String,
    pub protocol: String,
    pub sourceIp: String,
    pub userAgent: String,
}

#[derive(Serialize)]
pub struct Response {
    pub statusCode: i32,
    pub headers: HashMap<String,String>,
    pub body: String,
    //pub cookies: HashMap<String,String>,
    pub isBase64Encoded: bool
}

pub async fn process_request(request: Request) -> Result<Response, Error> {

    debug!("Request: {:?}", request);

    let url_path = &request.requestContext.http.path;

    if url_path.starts_with("/find-at/") {
        let response = handle_find_at(&request).await;
        return Ok(response);
    }

    debug!("No handler found");
    Ok(Response {
        statusCode: 404,
        headers: HashMap::from([("Content-Type".to_string(), "text/plain".to_string())]),
        body: "No handler found".to_string(),
        isBase64Encoded: false
    })
}

async fn handle_find_at(req: &Request) -> Response {
    let url_path = req.requestContext.http.path.clone();
    let s3_subpath: String = url_path.replace("/find-at/", "");
    let s3_prefix = format!("{}/{}", lambda_env::S3_BASE_PATH.as_str(), s3_subpath);

    let column_path = req.queryStringParameters["column"]
        .split(".")
        .map(|s|s.to_owned())
        .collect::<Vec<String>>();

    let values = req.queryStringParameters["values"]
        .split(",")
        .map(|s|s.to_owned())
        .collect::<Vec<String>>();

    debug!("S3 prefix: {}, column: '{:?}', values: {:?}", s3_prefix, column_path, values);

    let result = search_in_s3_dir(&s3_prefix, &column_path, &values).await;

    if let Some(data) = result {
        let base64_str = BASE64_STANDARD.encode(&data);
        let encoded_len = base64_str.len();
        debug!("Downloading file, bytes={}, base64 len={}", data.len(), encoded_len);
        Response {
            statusCode: 200,
            headers: HashMap::from([
                ("Content-Type".to_string(), "application/avro".to_string()),
                ("Content-Disposition".to_string(), "attachment; filename=\"result.avro\"".to_string())
            ]),
            body: base64_str,
            isBase64Encoded: true
        }
    } else {
        debug!("No records to download");
        Response {
            statusCode: 404,
            headers: HashMap::new(),
            body: "No records found".to_string(),
            isBase64Encoded: false
        }
    }
}

async fn search_in_s3_dir(s3_prefix: &str, column_path: &[String], values: &[String]) -> Option<Vec<u8>> {
    let s3_files = s3_util::list_object_keys_with_filter(
        lambda_env::S3_BUCKET.as_str(), &s3_prefix, None, 
        &|obj: &&aws_sdk_s3::types::Object| { obj.key().map(|k|k.ends_with(".avro")).unwrap_or(false) }
    ).await;

    let mut schema_opt: Option<Schema> = None;
    let mut result: Vec<AvroValue> = Vec::new();

    for s3_file in s3_files {
        let bytes = s3_util::get_object_as_bytes(lambda_env::S3_BUCKET.as_str(), &s3_file).await;
        let (r, s) = avro_filter::process_avro(bytes.reader(), column_path, values);
        debug!("In file: {} found {} records", s3_file, r.len());
        schema_opt = Some(s);
        result.extend_from_slice(&r);
    }

    match schema_opt {
        Some(schema) => {
            let mut writer = Writer::new(&schema, Vec::new());
            for row in result {
                writer.append(row).unwrap();
            }
            let encoded = writer.into_inner();
            encoded.ok()
        },
        _ => None,
    }

}

