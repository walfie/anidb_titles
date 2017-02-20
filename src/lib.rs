#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;
extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

pub mod error;
use csv::NextField;
pub use error::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::Path;
pub mod elastic;

// Sorted by lowest priority to highest
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TitleType {
    Short,
    Synonym,
    Official,
    Primary,
}

impl TitleType {
    fn from_id(id: &str) -> Result<Self> {
        use TitleType::*;
        match id {
            "1" => Ok(Primary),
            "2" => Ok(Synonym),
            "3" => Ok(Short),
            "4" => Ok(Official),
            _ => Err(ErrorKind::InvalidTitleType(id.to_string()).into()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Title {
    pub id: u32,
    pub title_type: TitleType,
    pub language: String,
    pub title: String,
}

pub struct TitleIterator<'a> {
    reader: csv::Reader<File>,
    line_num: u32,
    languages: HashSet<&'a str>,
}

impl<'a> TitleIterator<'a> {
    pub fn new<P>(file_path: P, languages: &[&'a str]) -> Result<TitleIterator<'a>>
        where P: 'a + AsRef<Path>
    {
        let file = File::open(file_path)?;
        let mut reader = BufReader::new(file);

        let mut language_set = HashSet::new();
        for language in languages.iter() {
            language_set.insert(*language);
        }

        // Ignore first 3 lines, which are comments
        {
            let mut s = String::new();
            for _ in 0..3 {
                let _ = reader.read_line(&mut s);
            }
        }
        let line_num: u32 = 4;

        let csv_reader = csv::Reader::from_reader(reader.into_inner())
            .delimiter(b'|')
            .double_quote(false)
            .flexible(true) // For titles that contain the delimiter '|' in them
            .record_terminator(csv::RecordTerminator::Any(b'\n'));

        Ok(TitleIterator {
            reader: csv_reader,
            line_num: line_num,
            languages: language_set,
        })
    }
}

fn fail_parse<T>(line_num: u32) -> Option<Result<T>> {
    Some(Err(ErrorKind::InvalidParse(line_num).into()))
}

impl<'a> Iterator for TitleIterator<'a> {
    type Item = Result<Title>;

    fn next(&mut self) -> Option<Self::Item> {
        let id: u32 = match self.reader.next_str() {
            NextField::Data(s) => {
                match s.parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        let kind = ErrorKind::InvalidId(s.to_string(), self.line_num);
                        return Some(Err(kind.into()));
                    }
                }
            }
            NextField::EndOfCsv => return None,
            _ => return fail_parse(self.line_num),
        };

        let title_type = match self.reader.next_str() {
            NextField::Data(s) => {
                match TitleType::from_id(s) {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                }
            }
            _ => return fail_parse(self.line_num),
        };

        let language_opt = match self.reader.next_str() {
            NextField::Data(s) => {
                if self.languages.contains(s) {
                    Some(s.to_string())
                } else {
                    None
                }
            }
            _ => return fail_parse(self.line_num),
        };

        if let Some(language) = language_opt {
            let mut title = match self.reader.next_str() {
                NextField::Data(s) => {
                    // This slows things down by a lot
                    s.replace("&lt;", "<").replace("&gt;", ">")
                }
                _ => return fail_parse(self.line_num),
            };

            loop {
                match self.reader.next_str() {
                    NextField::EndOfRecord => break,

                    // "Shin Evangelion Gekijouban:||" has "||" at the end, heck
                    NextField::Data(s) => {
                        title.push('|');
                        title.push_str(s);
                    }

                    _ => return fail_parse(self.line_num),
                }
            }

            self.line_num += 1;

            return Some(Ok(Title {
                id: id,
                title_type: title_type,
                language: language,
                title: title,
            }));
        } else {
            // Language doesn't match, so ignore the rest of this row
            loop {
                match self.reader.next_str() {
                    NextField::EndOfRecord => break,
                    NextField::Data(_) => continue,
                    _ => return fail_parse(self.line_num),
                }
            }

            self.line_num += 1;

            return self.next();
        }
    }
}
