// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write;
use std::rc::Rc;

use anyhow::{Error, Result};
use datadriven::walk;
use serde_json::Value;

use persister::persister::{Directory, Persister};
use repr::{Datum, Row};

struct MockFs {
    files: Rc<RefCell<HashMap<String, Vec<u8>>>>,
    events: Rc<RefCell<Vec<String>>>,
}

impl Directory for MockFs {
    // TODO: s/String/Filename/
    fn list(&self) -> Result<Vec<String>, Error> {
        Ok(self.files.borrow().keys().cloned().collect())
    }

    fn read(&self, fname: &str) -> Result<Vec<u8>, Error> {
        Ok(self.files.borrow().get(fname).unwrap().clone())
    }

    // TODO make this interface streaming
    fn write_to(&mut self, fname: &str, data: Vec<u8>) -> Result<(), Error> {
        Ok(self
            .events
            .borrow_mut()
            .push(format!("wrote {} bytes to {}", data.len(), fname)))
    }

    fn append_to_manifest(
        &mut self,
        source_name: &str,
        fname: &str,
        from: usize,
        to: usize,
    ) -> Result<(), Error> {
        Ok(self.events.borrow_mut().push(format!(
            "appended to manifest: {}: {} [{}, {})",
            fname, source_name, from, to
        )))
    }
}

#[test]
fn datadriven() {
    walk("tests/testdata", |f| {
        // Super crummy "mock filesystem."
        let files = Rc::new(RefCell::new(HashMap::new()));
        // Anything the persister tries to do will instead get logged as an "event" here to be
        // printed out.
        let events = Rc::new(RefCell::new(vec![]));

        let mut persister = Persister::new_raw(MockFs {
            files: files.clone(),
            events: events.clone(),
        });

        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "write-file" => {
                    let input: Value = serde_json::from_str(&test_case.input).unwrap();
                    let ary = input.as_array().unwrap();
                    let mut out: Vec<u8> = Vec::new();
                    for record in ary {
                        let mut row_data = Vec::new();

                        for s in record["data"].as_array().unwrap() {
                            row_data.push(Datum::String(s.as_str().unwrap().clone()));
                        }

                        Row::pack(&row_data).encode(&mut out);

                        Row::pack(&[
                            Datum::Int64(record["offset"].as_i64().unwrap()),
                            Datum::Int64(record["time"].as_i64().unwrap()),
                            Datum::Int64(record["diff"].as_i64().unwrap()),
                        ])
                        .encode(&mut out);
                    }
                    files
                        .borrow_mut()
                        .insert(test_case.args.get("name").unwrap()[0].clone(), out);
                }
                "awake" => {
                    persister.awake().unwrap();
                }
                _ => {}
            }
            let mut out = String::new();
            for ev in events.borrow_mut().drain(..) {
                write!(out, "{}\n", ev).unwrap();
            }
            out
        });
    });
}
