mod aws_util;
mod s3_util;
mod string_util;
mod avro_filter;
mod http_router;
mod lambda_env;

use http_router::*;

use lambda_runtime::{run, service_fn, Error, LambdaEvent};


#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false) // disable printing the name of the module in every log line.
        .without_time() // disabling time is handy because CloudWatch will add the ingestion time.
        .with_file(false)
        .with_ansi(false)
        .with_line_number(false)
        .with_thread_ids(false)
        .with_target(false)
        .with_thread_names(false)
        .init();

    run(service_fn(function_handler)).await
}

async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    process_request(event.payload).await
}
