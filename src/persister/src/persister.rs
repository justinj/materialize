// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use byteorder::{ByteOrder, NetworkEndian};
use std::collections::HashSet;
use std::fs;

use repr::{Datum, Row};

pub trait Directory {
    // TODO: s/String/Filename/
    fn list(&self) -> Vec<String>;
    fn read(&self, fname: &str) -> Vec<u8>;
    fn processed_files(&self) -> HashSet<String>;

    // TODO make this interface streaming
    fn write_to(&mut self, s: String, data: Vec<u8>);
}

#[derive(Debug, Clone)]
pub struct Persister<T: Directory> {
    dir: T,
}

impl<T: Directory> Persister<T> {
    pub fn new_raw(dir: T) -> Self {
        Persister { dir }
    }

    fn encode_records(data: Vec<Record>) -> Vec<u8> {
        // TODO: reuse ruchir's code for this.
        let mut buf = Vec::new();
        for rec in data {
            rec.row.encode(&mut buf);
            let metadata_row = Row::pack(&[
                Datum::Int64(rec.position),
                Datum::Int64(rec.time),
                Datum::Int64(rec.diff),
            ]);
            metadata_row.encode(&mut buf);
        }
        buf
    }

    pub fn run(&mut self) {
        let processed = self.dir.processed_files();
        for f in self.dir.list() {
            if !processed.contains(&f) {
                let iter = RecordIter {
                    data: self.dir.read(&f),
                };
                let mut recs = iter.collect::<Vec<Record>>();
                recs.sort_by(|a, b| a.time.cmp(&b.time));
                self.dir.write_to(f.clone(), Self::encode_records(recs));
            }
        }
    }
}

pub struct RecordIter {
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Record {
    row: Row,
    position: i64,
    time: i64,
    diff: i64,
}

impl RecordIter {
    fn next_rec(&mut self) -> Row {
        let len = NetworkEndian::read_u32(&self.data);
        let (_, mut rest) = self.data.split_at_mut(4);
        let (row, data) = rest.split_at_mut(len as usize);
        let row = row.to_vec().clone();
        // TODO: slow! figure out how not to copy here?
        self.data = data.to_vec();
        Row::decode(row)
    }
}

impl Iterator for RecordIter {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self.data.len() == 0 {
            return None;
        }
        let row = self.next_rec();
        let meta_row = self.next_rec();
        let meta = meta_row.unpack();
        let position = meta[0].unwrap_int64();
        let time = meta[1].unwrap_int64();
        let diff = meta[2].unwrap_int64();
        Some(Record {
            row,
            position,
            time,
            diff,
        })
    }
}

struct DirPersister {
    raw_dir: String,
    processed_dir: String,
}

impl DirPersister {
    pub fn new(raw_dir: String, processed_dir: String) -> Self {
        DirPersister {
            raw_dir,
            processed_dir,
        }
    }

    fn contents(&self, dir: &str) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path().to_str().unwrap().into())
            .collect()
    }
}

impl Directory for DirPersister {
    fn list(&self) -> Vec<String> {
        // TODO: no unwrap, this thing needs to return an error
        self.contents(&self.raw_dir)
    }

    fn read(&self, fname: &str) -> Vec<u8> {
        // TODO: no unwrap, this thing needs to return an error
        fs::read(fname).unwrap()
    }

    fn processed_files(&self) -> HashSet<String> {
        self.contents(&self.processed_dir).iter().cloned().collect()
    }

    fn write_to(&mut self, s: String, data: Vec<u8>) {}
}
