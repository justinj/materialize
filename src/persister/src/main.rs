// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use persister::persister::{DirPersister, Persister};

fn main() {
    let mut p = Persister::new_raw(DirPersister {
        raw_dir: "./mzdata/persistence-raw/".into(),
        processed_dir: "./mzdata/persistence-processed/".into(),
    });

    p.awake().unwrap();
}
