// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use byteorder::{ByteOrder, NetworkEndian};
use std::collections::{HashMap, HashSet};
use std::{env, fs};

use repr::{Datum, Row};

pub trait Directory {
    // TODO: s/String/Filename/
    fn list(&self) -> Vec<String>;
    fn read(&self, fname: &str) -> Vec<u8>;

    // TODO make this interface streaming
    fn write_to(&mut self, s: String, data: Vec<u8>);
}

#[derive(Debug, Clone)]
struct SourceState {
    last_offset_persisted: i64,
    records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct Persister<T: Directory> {
    dir: T,
    sources: HashMap<String, SourceState>,
    read_files: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct InputFile {
    source_id: String,
    worker_id: String,
    startup_time: String,
    nonce: String,
    seqnum: usize,
}

impl InputFile {
    fn from_fname(input: &str) -> InputFile {
        // pretty hacky bud.
        let parts: Vec<&str> = input.split('-').collect();
        InputFile {
            source_id: parts[3].into(),
            worker_id: parts[4].into(),
            startup_time: parts[5].into(),
            nonce: parts[6].into(),
            seqnum: parts[7].parse().unwrap(),
        }
    }
}

impl<T: Directory> Persister<T> {
    pub fn new_raw(dir: T) -> Self {
        Persister {
            dir,
            sources: HashMap::new(),
            read_files: HashSet::new(),
        }
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
        for f in self.dir.list() {
            if !self.read_files.contains(&f) {
                let meta = InputFile::from_fname(&f);
                if !self.sources.contains_key(&meta.source_id) {
                    self.sources.insert(
                        meta.source_id.clone(),
                        SourceState {
                            last_offset_persisted: 0,
                            records: Vec::new(),
                        },
                    );
                }
                let iter = RecordIter {
                    data: self.dir.read(&f),
                    idx: 0,
                };
                self.sources
                    .get_mut(&meta.source_id)
                    .unwrap()
                    .records
                    .extend(iter);
                self.read_files.insert(f);
            }
        }
    }

    pub fn flush(&mut self) {
        for name in self.sources.keys().cloned().collect::<Vec<String>>() {
            let mut entry = self.sources.get_mut(&name).unwrap();
            entry.records.sort_by(|a, b| a.position.cmp(&b.position));
            let mut prefix_len = 0;
            let mut to_emit = Vec::new();
            let mut new_recs = Vec::new();
            let starting_from = entry.last_offset_persisted;
            for (i, rec) in entry.records.drain(..).enumerate() {
                if rec.position == entry.last_offset_persisted + 1 {
                    entry.last_offset_persisted += 1;
                    prefix_len = i + 1;
                    to_emit.push(rec);
                } else {
                    new_recs.push(rec);
                }
            }
            if to_emit.len() > 0 {
                // TODO generate a sane name here
                self.dir.write_to(
                    format!(
                        "materialized-source-{}-{}-{}",
                        name,
                        starting_from + 1,
                        entry.last_offset_persisted + 1
                    ),
                    Self::encode_records(to_emit),
                );
            }
            entry.records = new_recs;
        }
    }
}

pub struct RecordIter {
    data: Vec<u8>,
    idx: usize,
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
        let (_, data) = self.data.split_at(self.idx);
        let len = NetworkEndian::read_u32(data) as usize;
        let (_, data) = data.split_at(4);
        let (row, _) = data.split_at(len);
        self.idx += 4 + len;
        Row::decode(row.to_vec().clone())
    }
}

impl Iterator for RecordIter {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        if self.data.len() <= self.idx {
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

pub struct DirPersister {
    pub raw_dir: String,
    pub processed_dir: String,
}

impl DirPersister {
    pub fn new(raw_dir: String, processed_dir: String) -> Self {
        DirPersister {
            raw_dir,
            processed_dir,
        }
    }

    fn contents(&self, dir: &str) -> Vec<String> {
        // TODO: handle dir not existing
        let dir = fs::read_dir(dir).unwrap();

        dir.map(|e| e.unwrap().path().to_str().unwrap().into())
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

    fn write_to(&mut self, s: String, data: Vec<u8>) {
        fs::write(s, data);
    }
}
