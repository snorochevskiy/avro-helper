use lazy_static::lazy_static;

lazy_static! {
    pub static ref S3_BUCKET: String = std::env::var("S3_BUCKET").unwrap();
    pub static ref S3_BASE_PATH: String = std::env::var("S3_BASE_PATH").unwrap();
    pub static ref AUTH_SECRET: String = std::env::var("AUTH_SECRET").unwrap();
}