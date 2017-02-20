#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;

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

impl Title {
    fn from_line(line: &str) -> Result<Self> {
        let mut parts = line.split('|');

        if let (Some(id), Some(title_type), Some(language), Some(title)) =
            (parts.next(), parts.next(), parts.next(), parts.next()) {
            let id = id.parse::<u32>().map_err(|_| ErrorKind::InvalidId(id.to_string()))?;
            let title_type = TitleType::from_id(title_type)?;
            Ok(Title {
                id: id,
                title_type: title_type,
                language: language.into(),
                title: title.into(),
            })
        } else {
            Err(ErrorKind::InvalidLine(line.to_string()).into())
        }
    }
}

pub fn process_lines<I>(lines: I, languages: &[&str]) -> Result<HashMap<u32, Vec<Title>>>
    where I: Iterator<Item = String>
{
    let mut set = HashSet::new();
    for language in languages.iter() {
        set.insert(language.to_string());
    }

    let mut titles: HashMap<u32, Vec<Title>> = HashMap::new();

    let iter = lines.skip_while(|l| l.starts_with('#'));

    for line in iter {
        let title = Title::from_line(&line)?;

        use std::collections::hash_map::Entry;

        if set.contains(&title.language) {
            match titles.entry(title.id) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(title);
                }
                Entry::Vacant(v) => {
                    v.insert(vec![title]);
                }
            };
        }
    }

    Ok(titles)
}

pub fn process_file<P>(file_path: P, languages: &[&str]) -> Result<HashMap<u32, Vec<Title>>>
    where P: AsRef<Path>
{
    let f = File::open(file_path)?;
    let file = BufReader::new(&f);

    process_lines(file.lines().flat_map(|l| l.ok()), languages)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_line() {
        assert_eq!(Title::from_line("9348|2|en|Aikatsu! Idol Activities!").ok(),
                   Some(Title {
                       id: 9348,
                       title_type: TitleType::Synonym,
                       language: "en",
                       title: "Aikatsu! Idol Activities!",
                   }));

        assert_eq!(Title::from_line("9348|4|ja|アイカツ! アイドルカツドウ!").ok(),
                   Some(Title {
                       id: 9348,
                       title_type: TitleType::Official,
                       language: "ja",
                       title: "アイカツ! アイドルカツドウ!",
                   }));

        assert!(Title::from_line("1234|5|ja|5 is an invalid title type").is_err());
        assert!(Title::from_line("1234|4|this doesn't have enough columns").is_err());
    }
}
