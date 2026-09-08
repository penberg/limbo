#![deny(clippy::all)]

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use turso_sync_engine::{
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    types::{DatabaseRowTransformResult, DatabaseStatementReplay},
};

use crate::{
    core_change_type_to_js, core_values_map_to_js, js_value_to_core, DatabaseRowMutationJs,
    DatabaseRowTransformResultJs,
};

#[napi]
pub enum JsProtocolRequest {
    Http {
        url: Option<String>,
        method: String,
        path: String,
        body: Option<Uint8Array>,
        headers: Vec<(String, String)>,
    },
    FullRead {
        path: String,
    },
    FullWrite {
        path: String,
        content: Uint8Array,
    },
    Transform {
        mutations: Vec<DatabaseRowMutationJs>,
    },
}

#[derive(Clone)]
#[napi]
pub struct JsDataCompletion(Arc<Mutex<JsDataCompletionInner>>);

pub struct JsBytesPollResult(Buffer);

impl DataPollResult<u8> for JsBytesPollResult {
    fn data(&self) -> &[u8] {
        &self.0
    }
}
pub struct JsTransformPollResult(Vec<DatabaseRowTransformResult>);

impl DataPollResult<DatabaseRowTransformResult> for JsTransformPollResult {
    fn data(&self) -> &[DatabaseRowTransformResult] {
        &self.0
    }
}

struct JsDataCompletionInner {
    status: Option<u16>,
    chunks: VecDeque<Buffer>,
    transformed: VecDeque<DatabaseRowTransformResult>,
    finished: bool,
    err: Option<String>,
}

impl JsDataCompletion {
    fn inner(&self) -> turso_sync_engine::Result<MutexGuard<JsDataCompletionInner>> {
        let inner = self.0.lock().unwrap();
        if let Some(err) = &inner.err {
            return Err(turso_sync_engine::errors::Error::DatabaseSyncEngineError(
                err.clone(),
            ));
        }
        Ok(inner)
    }
}

impl DataCompletion<u8> for JsDataCompletion {
    type DataPollResult = JsBytesPollResult;

    fn status(&self) -> turso_sync_engine::Result<Option<u16>> {
        let inner = self.inner()?;
        Ok(inner.status)
    }

    fn poll_data(&self) -> turso_sync_engine::Result<Option<Self::DataPollResult>> {
        let mut inner = self.inner()?;
        let chunk = inner.chunks.pop_front();
        Ok(chunk.map(JsBytesPollResult))
    }

    fn is_done(&self) -> turso_sync_engine::Result<bool> {
        let inner = self.inner()?;
        Ok(inner.finished)
    }
}

impl DataCompletion<DatabaseRowTransformResult> for JsDataCompletion {
    type DataPollResult = JsTransformPollResult;

    fn status(&self) -> turso_sync_engine::Result<Option<u16>> {
        let inner = self.inner()?;
        Ok(inner.status)
    }

    fn poll_data(&self) -> turso_sync_engine::Result<Option<Self::DataPollResult>> {
        let mut inner = self.inner()?;
        let chunk = inner.transformed.drain(..).collect::<Vec<_>>();
        if chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some(JsTransformPollResult(chunk)))
        }
    }

    fn is_done(&self) -> turso_sync_engine::Result<bool> {
        let inner = self.inner()?;
        Ok(inner.finished)
    }
}

#[napi]
impl JsDataCompletion {
    #[napi]
    pub fn poison(&self, err: String) {
        let mut completion = self.0.lock().unwrap();
        completion.err = Some(err);
    }

    #[napi]
    pub fn status(&self, value: u32) {
        let mut completion = self.0.lock().unwrap();
        completion.status = Some(value as u16);
    }

    #[napi]
    pub fn push_buffer(&self, value: Buffer) {
        let mut completion = self.0.lock().unwrap();
        completion.chunks.push_back(value);
    }

    #[napi]
    pub fn push_transform(&self, values: Vec<DatabaseRowTransformResultJs>) {
        let mut completion = self.0.lock().unwrap();
        for value in values {
            completion.transformed.push_back(match value {
                DatabaseRowTransformResultJs::Keep => DatabaseRowTransformResult::Keep,
                DatabaseRowTransformResultJs::Skip => DatabaseRowTransformResult::Skip,
                DatabaseRowTransformResultJs::Rewrite { stmt } => {
                    DatabaseRowTransformResult::Rewrite(DatabaseStatementReplay {
                        sql: stmt.sql,
                        values: stmt.values.into_iter().map(js_value_to_core).collect(),
                    })
                }
            });
        }
    }

    #[napi]
    pub fn done(&self) {
        let mut completion = self.0.lock().unwrap();
        completion.finished = true;
    }
}

#[napi]
pub struct JsProtocolRequestBytes {
    request: Arc<Mutex<Option<JsProtocolRequest>>>,
    completion: JsDataCompletion,
}

#[napi]
impl JsProtocolRequestBytes {
    #[napi]
    pub fn request(&self) -> JsProtocolRequest {
        let mut request = self.request.lock().unwrap();
        request.take().unwrap()
    }
    #[napi]
    pub fn completion(&self) -> JsDataCompletion {
        self.completion.clone()
    }
}

impl SyncEngineIo for JsProtocolIo {
    type DataCompletionBytes = JsDataCompletion;
    type DataCompletionTransform = JsDataCompletion;

    fn http(
        &self,
        url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> turso_sync_engine::Result<JsDataCompletion> {
        Ok(self.add_request(JsProtocolRequest::Http {
            url: url.map(|x| x.to_string()),
            method: method.to_string(),
            path: path.to_string(),
            body: body.map(Uint8Array::from),
            headers: headers
                .iter()
                .map(|x| (x.0.to_string(), x.1.to_string()))
                .collect(),
        }))
    }

    fn full_read(&self, path: &str) -> turso_sync_engine::Result<Self::DataCompletionBytes> {
        Ok(self.add_request(JsProtocolRequest::FullRead {
            path: path.to_string(),
        }))
    }

    fn full_write(
        &self,
        path: &str,
        content: Vec<u8>,
    ) -> turso_sync_engine::Result<Self::DataCompletionBytes> {
        Ok(self.add_request(JsProtocolRequest::FullWrite {
            path: path.to_string(),
            content: Uint8Array::from(content),
        }))
    }

    fn transform(
        &self,
        mutations: Vec<turso_sync_engine::types::DatabaseRowMutation>,
    ) -> turso_sync_engine::Result<Self::DataCompletionTransform> {
        Ok(self.add_request(JsProtocolRequest::Transform {
            mutations: mutations
                .into_iter()
                .filter_map(|mutation| {
                    let change_type = core_change_type_to_js(mutation.change_type)?;
                    Some(DatabaseRowMutationJs {
                        change_time: mutation.change_time as i64,
                        table_name: mutation.table_name,
                        id: mutation.id,
                        change_type,
                        before: mutation.before.map(core_values_map_to_js),
                        after: mutation.after.map(core_values_map_to_js),
                        updates: mutation.updates.map(core_values_map_to_js),
                    })
                })
                .collect(),
        }))
    }

    fn add_io_callback(&self, callback: Box<dyn FnMut() -> bool + Send>) {
        let mut work = self.work.lock().unwrap();
        work.push_back(callback);
    }

    fn step_io_callbacks(&self) {
        let mut items = {
            let mut work = self.work.lock().unwrap();
            work.drain(..).collect::<VecDeque<_>>()
        };
        let length = items.len();
        for _ in 0..length {
            let mut item = items.pop_front().unwrap();
            if item() {
                continue;
            }
            items.push_back(item);
        }
        {
            let mut work = self.work.lock().unwrap();
            work.extend(items);
        }
    }
}

#[napi]
pub struct JsProtocolIo {
    requests: Mutex<Vec<JsProtocolRequestBytes>>,
    work: Mutex<VecDeque<Box<dyn FnMut() -> bool + Send>>>,
}

impl Default for JsProtocolIo {
    fn default() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            work: Mutex::new(VecDeque::new()),
        }
    }
}

impl JsProtocolIo {
    pub fn take_request(&self) -> Option<JsProtocolRequestBytes> {
        self.requests.lock().unwrap().pop()
    }

    fn add_request(&self, request: JsProtocolRequest) -> JsDataCompletion {
        let completion = JsDataCompletionInner {
            chunks: VecDeque::new(),
            transformed: VecDeque::new(),
            finished: false,
            err: None,
            status: None,
        };
        let completion = JsDataCompletion(Arc::new(Mutex::new(completion)));

        let mut requests = self.requests.lock().unwrap();
        requests.push(JsProtocolRequestBytes {
            request: Arc::new(Mutex::new(Some(request))),
            completion: completion.clone(),
        });
        completion
    }
}
