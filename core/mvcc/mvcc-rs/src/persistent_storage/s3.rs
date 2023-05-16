use crate::database::{LogRecord, Result};
use crate::errors::DatabaseError;
use aws_sdk_s3::Client;

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Options {
    pub create_bucket_if_not_exists: bool,
}

impl Options {
    pub fn with_create_bucket_if_not_exists(create_bucket_if_not_exists: bool) -> Self {
        Self {
            create_bucket_if_not_exists,
        }
    }
}

#[derive(Debug)]
pub struct Replicator {
    pub client: Client,
    pub bucket: String,
    pub prefix: String,
}

impl Replicator {
    pub async fn new(options: Options) -> Result<Self> {
        let mut loader = aws_config::from_env();
        if let Ok(endpoint) = std::env::var("MVCCRS_ENDPOINT") {
            loader = loader.endpoint_url(endpoint);
        }
        let sdk_config = loader.load().await;
        let config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();
        let bucket = std::env::var("MVCCRS_BUCKET").unwrap_or_else(|_| "mvccrs".to_string());
        let prefix = std::env::var("MVCCRS_PREFIX").unwrap_or_else(|_| "tx".to_string());
        let client = Client::from_conf(config);

        match client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => tracing::info!("Bucket {bucket} exists and is accessible"),
            Err(aws_sdk_s3::error::SdkError::ServiceError(err)) if err.err().is_not_found() => {
                if options.create_bucket_if_not_exists {
                    tracing::info!("Bucket {bucket} not found, recreating");
                    client
                        .create_bucket()
                        .bucket(&bucket)
                        .send()
                        .await
                        .map_err(|e| DatabaseError::Io(e.to_string()))?;
                } else {
                    tracing::error!("Bucket {bucket} does not exist");
                    return Err(DatabaseError::Io(err.err().to_string()));
                }
            }
            Err(e) => {
                tracing::error!("Bucket checking error: {e}");
                return Err(DatabaseError::Io(e.to_string()));
            }
        }

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    pub async fn replicate_tx(&self, record: LogRecord) -> Result<()> {
        let key = format!("{}-{:020}", self.prefix, record.tx_timestamp);
        tracing::trace!("Replicating {key}");
        let body = serde_json::to_vec(&record).map_err(|e| DatabaseError::Io(e.to_string()))?;
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| DatabaseError::Io(e.to_string()))?;
        tracing::trace!("Replicator response: {:?}", resp);
        Ok(())
    }

    pub async fn read_tx_log(&self) -> Result<Vec<LogRecord>> {
        let mut records: Vec<LogRecord> = Vec::new();
        // Read all objects from the bucket, one log record is stored in one object
        let mut next_token = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);
            if let Some(next_token) = next_token {
                req = req.continuation_token(next_token);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| DatabaseError::Io(e.to_string()))?;
            tracing::trace!("List objects response: {:?}", resp);
            if let Some(contents) = resp.contents {
                // read the record from s3 based on the object metadata (`contents`)
                // and store it in the `records` vector
                for object in contents {
                    let key = object.key.unwrap();
                    let resp = self
                        .client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(&key)
                        .send()
                        .await
                        .map_err(|e| DatabaseError::Io(e.to_string()))?;
                    tracing::trace!("Get object response: {:?}", resp);
                    let body = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| DatabaseError::Io(e.to_string()))?;
                    let record: LogRecord = serde_json::from_slice(&body.into_bytes())
                        .map_err(|e| DatabaseError::Io(e.to_string()))?;
                    records.push(record);
                }
            }
            if resp.next_continuation_token.is_none() {
                break;
            }
            next_token = resp.next_continuation_token;
        }
        tracing::trace!("Records: {records:?}");
        Ok(records)
    }
}
