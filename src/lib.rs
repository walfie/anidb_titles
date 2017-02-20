#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;
extern crate csv;

pub mod error;
pub use error::*;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::Path;

#[derive(Debug, PartialEq)]
pub enum TitleType {
    Primary,
    Synonym,
    Short,
    Official,
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

pub fn process_file<P>(file_path: P, languages: &[&str]) -> Result<HashMap<u32, Vec<Title>>>
    where P: AsRef<Path>
{
    let file = File::open(file_path.as_ref())?;
    let mut reader = BufReader::new(&file);

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
    let mut line_num: u32 = 4;

    let mut reader = csv::Reader::from_reader(reader)
        .delimiter(b'|')
        .flexible(true) // For titles that contain the delimiter '|' in them
        .record_terminator(csv::RecordTerminator::Any(b'\n'));

    let mut titles_hash_map: HashMap<u32, Vec<Title>> = HashMap::new();

    loop {
        use csv::NextField;

        let id: u32 = match reader.next_str() {
            NextField::Data(s) => {
                s.parse::<u32>()
                    .map_err(|_| ErrorKind::InvalidId(s.to_string(), line_num))?
            }
            NextField::EndOfCsv => break,
            _ => return fail_parse(file_path, line_num),
        };

        let title_type = match reader.next_str() {
            NextField::Data(s) => TitleType::from_id(s)?,
            _ => return fail_parse(file_path, line_num),
        };

        let language_opt = match reader.next_str() {
            NextField::Data(s) => {
                if language_set.contains(s) {
                    Some(s.to_string())
                } else {
                    None
                }
            }
            _ => return fail_parse(file_path, line_num),
        };

        if let Some(language) = language_opt {
            let mut title = match reader.next_str() {
                NextField::Data(s) => s.to_string(),
                _ => return fail_parse(file_path, line_num),
            };

            loop {
                match reader.next_str() {
                    NextField::EndOfRecord => break,

                    // "Shin Evangelion Gekijouban:||" has "||" at the end, heck
                    NextField::Data(s) => {
                        title.push('|');
                        title.push_str(s);
                    }

                    _ => return fail_parse(file_path, line_num),
                }
            }

            let new_title = Title {
                id: id,
                title_type: title_type,
                language: language,
                title: title,
            };

            use std::collections::hash_map::Entry;
            match titles_hash_map.entry(id) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(new_title);
                }
                Entry::Vacant(v) => {
                    v.insert(vec![new_title]);
                }
            };
        } else {
            // Language doesn't match, so ignore the rest of this row
            loop {
                match reader.next_str() {
                    NextField::EndOfRecord => break,
                    NextField::Data(_) => continue,
                    _ => return fail_parse(file_path, line_num),
                }
            }
        }

        line_num += 1;
    }

    Ok(titles_hash_map)
}

fn fail_parse<P, T>(file_path: P, line_num: u32) -> Result<T>
    where P: AsRef<Path>
{
    let path_str = file_path.as_ref().to_string_lossy().into();
    Err(ErrorKind::InvalidParse(path_str, line_num).into())
}
