// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A low level `JetStream` responses.

use core::fmt;

use serde::Deserialize;

/// An error description returned in a response to a jetstream request.
#[derive(Debug, Deserialize)]
pub struct Error {
    /// Error code
    #[serde(rename = "err_code")]
    pub code: u64,

    /// Status code
    #[serde(rename = "code")]
    pub status: u16,

    /// Description
    pub description: String,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JetStream response error code: {}, status: {}, description: {}",
            self.code, self.status, self.description
        )
    }
}

/// A response returned from a request to jetstream.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Response<T> {
    Err { error: Error },
    Ok(T),
}
