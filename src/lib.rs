#![recursion_limit = "1024"]
#[macro_use]
extern crate error_chain;
extern crate tantivy;

pub mod error;
pub use error::{Error, Result};

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufRead};

use tantivy::Index;

pub fn index() -> Result<Index> {
    use tantivy::schema::*;

    let mut schema_builder = SchemaBuilder::default();
    schema_builder.add_text_field("title", TEXT | STORED);

    let id_options = U32Options::default().set_stored().set_indexed();
    schema_builder.add_u32_field("id", id_options);

    let schema = schema_builder.build();

    // TODO: Don't create in ram
    Ok(Index::create_in_ram(schema))
}

// TODO: Stop using `unwrap()`
pub fn process_file(file_path: &str, languages: &[&str], index: Index) -> Result<Index> {
    use tantivy::Document;

    let mut index_writer = index.writer(100_000_000).unwrap(); // Preserve 100MB. TODO: Don't unwrap
    let schema = index.schema();

    let id_field = schema.get_field("id").unwrap();
    let title_field = schema.get_field("title").unwrap();

    let mut hash = HashSet::new();
    for language in languages.iter() {
        hash.insert(language);
    }

    let f = File::open(file_path)?;
    let file = BufReader::new(&f);

    for line in file.lines().flat_map(|l| l.ok()).skip_while(|l| l.starts_with('#')) {
        let mut parts = line.split('|');

        if let (Some(id), Some(title_type), Some(lang), Some(title)) =
            (parts.next(), parts.next(), parts.next(), parts.next()) {
            if hash.contains(&lang) {
                let mut doc = Document::default();
                doc.add_u32(id_field, id.parse::<u32>().unwrap()); // TODO: Don't unwrap
                doc.add_text(title_field, title);
                index_writer.add_document(doc); // Ignore error
                // println!("{} {} {} {}", id, title_type, lang, title);
            }
        }
    }

    index_writer.commit();

    Ok(index)
}
