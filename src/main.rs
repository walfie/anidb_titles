extern crate clubdam_anidb_indexer as indexer;
extern crate serde_json;
extern crate clubdarn;
extern crate itertools;

use indexer::Title;
use indexer::elastic;
use indexer::error::*;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

fn main() {
    let mut args = std::env::args().skip(1);
    let (path, url) = match (args.next(), args.next()) {
        (Some(path), Some(url)) => (path, url),
        _ => panic!("Invalid args"),
    };

    if let Err(e) = run(&path, &url) {
        use std::io::Write;

        let stderr = &mut std::io::stderr();
        let err_msg = "Error writing to stderr";

        writeln!(stderr, "error: {}", e).expect(err_msg);

        for e in e.iter().skip(1) {
            writeln!(stderr, "caused by: {}", e).expect(err_msg);
        }

        // If backtrace is generated (via `RUST_BACKTRACE=1`), print it
        if let Some(backtrace) = e.backtrace() {
            writeln!(stderr, "backtrace: {:?}", backtrace).expect(err_msg);
        }

        std::process::exit(1);
    }
}

fn run(path: &str, url: &str) -> Result<()> {
    let alias = "series";
    let search_client = elastic::Client::new(url, alias, "series")?;

    let darn = clubdarn::Client::default()?;

    println!("Getting series from ClubDAM");
    let series = darn.series()
        .by_category(clubdarn::category::series::ANIME)
        .send()
        .chain_err(|| "failed to get series from ClubDAM (maybe it's down?)")?;

    println!("Reindexing AniDB titles to Elasticsearch");
    let old_indices = reindex(&search_client, path)?;

    let languages = ["ja"];

    println!("Searching for ClubDAM series names in Elasticsearch");

    let batch_size = 500;

    let mut anidb_id_to_clubdam_titles: HashMap<String, Vec<String>> = HashMap::new();
    let mut clubdam_titles_not_in_anidb: Vec<elastic::Series> = Vec::new();

    for chunk in &series.items.into_iter().chunks(batch_size) {
        let series_batch = chunk.collect::<Vec<clubdarn::Series>>();
        let titles = series_batch.iter().map(|s| s.title.clone()).collect::<Vec<String>>();

        let search_results = search_client.multi_search(&titles, &languages)?;

        let zipped = series_batch.into_iter().zip(search_results);

        for (clubdarn_series, anidb_series_opt) in zipped {
            if let Some(anidb_series) = anidb_series_opt {
                // Series exists in ClubDAM and AniDB, we should update the
                // indexed docs to include the ClubDAM title
                match anidb_id_to_clubdam_titles.entry(anidb_series.id.to_string()) {
                    Entry::Occupied(mut o) => {
                        o.get_mut().push(clubdarn_series.title);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![clubdarn_series.title]);
                    }
                };
            } else {
                // Series exists in ClubDAM but not AniDB, we should insert
                // the ClubDAM titles into Elasticsearch
                let mut titles_map = HashMap::with_capacity(1);
                let mut titles_vec: Vec<String> = Vec::with_capacity(1);
                titles_vec.push(clubdarn_series.title.clone());
                titles_map.insert("clubdam".to_string(), titles_vec);

                let series = elastic::Series {
                    id: clubdarn_series.title.clone(),
                    main_title: Some(clubdarn_series.title),
                    titles: elastic::TitlesByLanguage(titles_map),
                };

                clubdam_titles_not_in_anidb.push(series);
            }
        }
    }

    println!("Updating existing Elasticsearch documents to include ClubDAM titles");
    for chunk in &anidb_id_to_clubdam_titles.drain().chunks(batch_size) {
        search_client.bulk_update(chunk, true)?;
    }

    println!("Updating Elasticsearch with unmatched ClubDAM titles");
    for chunk in &clubdam_titles_not_in_anidb.into_iter().chunks(batch_size) {
        search_client.bulk_insert(alias, chunk, true)?;
    }

    println!("Deleting non-ClubDAM documents");

    search_client.delete_non_clubdam(batch_size)?;

    println!("Deleting old Elasticsearch indices");

    search_client.delete_indices(&old_indices)
}

fn reindex(client: &elastic::Client, path: &str) -> Result<Vec<String>> {
    let titles_iter = indexer::TitleIterator::new(path)?;

    let mut titles_hash_map: HashMap<u32, Vec<Title>> = HashMap::new();

    for title_result in titles_iter {
        let title = title_result?;

        match titles_hash_map.entry(title.id) {
            Entry::Occupied(mut o) => {
                o.get_mut().push(title);
            }
            Entry::Vacant(v) => {
                v.insert(vec![title]);
            }
        };
    }

    let series = titles_hash_map.drain().map(|(id, titles)| {
        let titles_by_language = elastic::TitlesByLanguage::new(titles);
        let main_title = titles_by_language.main_title("ja");
        elastic::Series {
            id: id.to_string(),
            main_title: main_title,
            titles: titles_by_language,
        }
    });

    let chunk_size = 1000;
    let should_wait = true;
    client.reindex(series, chunk_size, should_wait)
}
