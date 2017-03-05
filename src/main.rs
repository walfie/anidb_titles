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
    let search_client = elastic::Client::new(url, "series", "series")?;

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

    let mut clubdam_map: HashMap<u32, Vec<String>> = HashMap::new();

    for chunk in &series.items.into_iter().chunks(batch_size) {
        let series_batch = chunk.collect::<Vec<clubdarn::Series>>();
        let titles = series_batch.iter().map(|s| s.title.clone()).collect::<Vec<String>>();

        let search_results = search_client.multi_search(&titles, &languages)?;

        let zipped = series_batch.into_iter()
            .zip(search_results)
            .filter_map(|(clubdarn, anidb_opt)| anidb_opt.map(|anidb| (clubdarn, anidb)));

        for (clubdarn_series, anidb_series) in zipped {
            match clubdam_map.entry(anidb_series.id) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(clubdarn_series.title);
                }
                Entry::Vacant(v) => {
                    v.insert(vec![clubdarn_series.title]);
                }
            };
        }
    }

    println!("Updating Elasticsearch with ClubDAM titles");
    for chunk in &clubdam_map.drain().chunks(batch_size) {
        search_client.bulk_update(chunk, true)?;
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
            id: id,
            main_title: main_title,
            titles: titles_by_language,
        }
    });

    let chunk_size = 1000;
    let should_wait = true;
    client.reindex(series, chunk_size, should_wait)
}
