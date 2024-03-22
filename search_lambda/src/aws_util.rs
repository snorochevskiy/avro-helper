use cached::proc_macro::cached;

use aws_config::SdkConfig;

#[cached]
pub fn get_config() -> SdkConfig {
    futures::executor::block_on(
        async {
            aws_config::load_from_env().await
        }
    )
}