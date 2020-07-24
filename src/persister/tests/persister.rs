// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use persister::persister::{DirPersister, Directory, Persister};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use repr::{Datum, Row};

#[derive(Debug, Clone)]
struct MockFile {
    name: String,
    data: Vec<u8>,
}

struct Dir {
    raw_dir: Rc<RefCell<Vec<MockFile>>>,
    written: Rc<RefCell<Vec<String>>>,
}

impl Directory for Dir {
    fn list(&self) -> Vec<String> {
        self.raw_dir
            .borrow()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    // TODO figure out how to stream this properly.
    fn read(&self, fname: &str) -> Vec<u8> {
        self.raw_dir
            .borrow()
            .iter()
            .find(|f| f.name == fname)
            .unwrap()
            .data
            .clone()
    }

    fn write_to(&mut self, file: String, _: Vec<u8>) {
        self.written.borrow_mut().push(file);
    }
}

#[test]
fn test_real() {
    let mut p = Persister::new_raw(DirPersister {
        raw_dir: "./mzdata/persistence-raw/".into(),
        processed_dir: "./mzdata/persistence-processed/".into(),
    });

    p.run();
    p.flush();
}

// #[test]
// fn test_persistence() {
//     let raw_dir = Rc::new(RefCell::new(vec![]));
//     let output_dir = Rc::new(RefCell::new(vec![]));

//     let mut p = Persister::new_raw(Dir {
//         raw_dir: raw_dir.clone(),
//         written: output_dir.clone(),
//     });

//     for _i in 0..10 {
//         p.run();
//     }

//     assert_eq!(*output_dir.borrow(), Vec::<String>::new());

//     let mut foo_contents = Vec::new();
//     Row::pack(&[Datum::Int64(1), Datum::Int64(2), Datum::Int64(1)]).encode(&mut foo_contents);
//     Row::pack(&[Datum::Int64(1), Datum::Int64(2), Datum::Int64(1)]).encode(&mut foo_contents);

//     raw_dir.borrow_mut().push(MockFile {
//         name: "foo".to_string(),
//         data: foo_contents,
//     });
//     for _ in 0..10 {
//         p.run();
//     }
//     p.flush();

//     assert_eq!(*output_dir.borrow(), vec!["outfile-1-2".to_string()]);

//     let mut bar_contents = Vec::new();
//     Row::pack(&[
//         Datum::String("hello!"),
//         Datum::String("world!"),
//         Datum::Int64(3),
//     ])
//     .encode(&mut bar_contents);
//     Row::pack(&[Datum::Int64(2), Datum::Int64(8), Datum::Int64(1)]).encode(&mut bar_contents);
//     Row::pack(&[
//         Datum::String("goodbye :("),
//         Datum::String("world!"),
//         Datum::Int64(3),
//     ])
//     .encode(&mut bar_contents);
//     Row::pack(&[Datum::Int64(3), Datum::Int64(5), Datum::Int64(9)]).encode(&mut bar_contents);

//     raw_dir.borrow_mut().push(MockFile {
//         name: "bar".to_string(),
//         data: bar_contents,
//     });
//     raw_dir.borrow_mut().push(MockFile {
//         name: "baz".to_string(),
//         data: vec![],
//     });
//     for _ in 0..10 {
//         p.run();
//     }
//     p.flush();

//     assert_eq!(
//         *output_dir.borrow(),
//         vec!["outfile-1-2".to_string(), "outfile-2-4".to_string()]
//     );
// }
