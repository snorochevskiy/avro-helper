use cached::proc_macro::cached;

use crate::string_util::StrUtil;
use crate::aws_util;
use aws_sdk_s3 as s3;

use std::io::Write;
use std::fs::File;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::types::{Delete, Object, ObjectIdentifier};
use bytes::Bytes;
use url::Url;

#[cached]
fn get_s3_client() -> s3::Client {
    let client = s3::Client::new(&aws_util::get_config());
    client
}

/// Used to list "names" in a "directory".
/// E.g. we have following objects:
/// - s3://mybucket/statuses/customer=A/dt=2021-10-10/1.avro
/// - s3://mybucket/statuses/customer=A/dt=2021-10-11/2.avro
/// - s3://mybucket/statuses/customer=B/dt=2021-10-10/3.avro
/// Call list_common_parts("mybucket", "statuses/customer=", "/") will return ["A", "B"]
pub async fn list_common_parts(bucket: &str, prefix: &str, delim: &str) -> Vec<String> {
    let client = get_s3_client();
    let request = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter(delim);

    #[async_recursion::async_recursion]
    async fn call_list_objects(request: ListObjectsV2FluentBuilder, mut result: Vec<String>) -> Vec<String> {
        let response = request.clone().send().await.unwrap();

        let customers = response.common_prefixes().iter()
            .flat_map(|o| o.prefix())
            .map(|s| s.extract_middle(response.prefix().unwrap(), response.delimiter().unwrap()))
            .collect::<Vec<_>>();

        result.extend_from_slice(customers.as_slice());

        if let Some(token) = response.next_continuation_token() {
            call_list_objects(request.continuation_token(token), result).await
        } else {
            result
        }
    }

    call_list_objects(request, Vec::new()).await
}

pub async fn list_object_keys(bucket: &str, prefix: &str) -> Vec<String> {
    list_object_keys_with_filter(bucket, prefix, None,&|x:&&Object| true).await
}


pub async fn list_object_keys_with_filter(bucket: &str, prefix: &str, soft_limit: Option<i32>, predicate: &dyn Fn(&&Object) -> bool) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();

    let client = get_s3_client();

    let mut request = client.list_objects_v2()
        .prefix(prefix)
        .bucket(bucket);

    loop {
        let response = request.clone().send().await.unwrap();

        let customers = response.contents().into_iter()
            .filter(predicate)
            .flat_map(|o| o.key().map(String::from))
            .collect::<Vec<String>>();

        result.extend_from_slice(customers.as_slice());

        if soft_limit.map(|l| l <= (result.len() as i32)) == Some(true) {
            break;
        } else if let Some(token) = response.next_continuation_token() {
            request = request.continuation_token(token);
        } else {
            break;
        }
    };
    result
}

/// Checks whether given prefix exists in S3 bucket
pub async fn prefix_exists(bucket: &str, prefix: &str) -> bool {
    let client = get_s3_client();
    let objects = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send().await
        .unwrap();
    !objects.contents().is_empty()
}

/// Downloads given S3 object, places it to /tmp/ folder and return a full name of the downloaded file.
/// Note, that AwS lambda comes with 512 MB of tmp storage.
pub async fn download_object(bucket: &str, key: &str)  -> String {
    let client = get_s3_client();
    let result = client.get_object()
        .bucket(bucket)
        .key(key)
        .send().await
        .unwrap();

    let body_bytes = result.body.collect().await.unwrap().into_bytes();

    let file_name = format!("/tmp/{}", key.extract_after_last("/"));

    let mut file = File::create(&file_name).expect("create failed");
    file.write_all(&body_bytes).expect(&format!("Failed to write the body of {}", &file_name));

    file_name
}

pub async fn get_object_as_bytes(bucket: &str, key: &str) -> Bytes {
    let client = get_s3_client();
    let result = client.get_object()
        .bucket(bucket)
        .key(key)
        .send().await
        .unwrap();

    let body_bytes = result.body.collect().await.unwrap().into_bytes();
    body_bytes
}

pub async fn delete_objects<S>(bucket: &str, keys: &[S]) where S: AsRef<str> {
    let client = get_s3_client();
    let mut it = keys.iter();
    loop { // AWS API can remove up to 10 objects at one call
        let mut portion = it.by_ref().take(10).peekable();
        if portion.peek().is_none() {
            break;
        }

        let mut to_delete: Vec<ObjectIdentifier> = vec![];
        for obj in portion {
             if let Ok(obj_id) = ObjectIdentifier::builder()
                .key(obj.as_ref())
                .build() {
                    to_delete.push(obj_id);
                }
        }
        if let Ok(del) = Delete::builder().set_objects(Some(to_delete)).quiet(true).build() {
            let result = client.delete_objects()
            .bucket(bucket)
            .delete(del)
            .send()
            .await;

        if let Err(e) = result {
            println!("Error copying: {}", e);
        }
        }
    }
}


pub async fn copy_object(bucket: &str, src_key: &str, dst_key: &str) -> bool {
    let mut source_bucket_and_object: String = "".to_owned();
    source_bucket_and_object.push_str(bucket);
    source_bucket_and_object.push('/');
    source_bucket_and_object.push_str(src_key);

    let client = get_s3_client();
    let result = client.copy_object()
        .bucket(bucket)
        .copy_source(source_bucket_and_object)
        .key(dst_key)
        .send()
        .await;

    match result {
        Ok(_) => true,
        Err(e) => {
            println!("Error copying: {}", e);
            false
        },
    }
}

pub async fn move_object(bucket: &str, src_key: &str, dst_key: &str) {
    if copy_object(bucket, src_key, dst_key).await {
        delete_objects(bucket, &[src_key]).await;
    }
}

pub async fn move_object_to(bucket: &str, src_key: &str, dir: &str) {
    let dst_key = format!("{}/{}", dir.trim_right_slash(), src_key);
    move_object(bucket, src_key, &dst_key).await;
}

pub struct S3Uri {
  bucket: String,
  path: String
}

impl S3Uri {
    pub fn parse(uri: &str) -> Option<S3Uri> {
        Url::parse(uri)
            .map(|url| {
                url.domain()
                    .map(|d| S3Uri{ bucket: d.to_string(), path: url.path().trim_left_slash().to_string()})
            })
            .ok()
            .flatten()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn path(&self) -> &str {
        &self.path
    }
}