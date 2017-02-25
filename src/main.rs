extern crate anidb_titles as titles;
extern crate serde_json;
extern crate clubdarn;
extern crate itertools;

use titles::elastic;
use titles::error::*;

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

    //reindex(&search_client, path)?;

    let darn = clubdarn::Client::default()?;
    let series = darn.series().by_category(clubdarn::category::series::ANIME).send()?;

    let languages = ["ja"];

    use itertools::Itertools;
    for chunk in &series.items.into_iter().chunks(500) {
        let series_batch = chunk.collect::<Vec<clubdarn::Series>>();
        let titles = series_batch.iter().map(|s| s.title.clone()).collect::<Vec<String>>();

        let search_results = search_client.multi_search(&titles, &languages)?;

        let zipped = series_batch.into_iter().zip(search_results);
        let docs = zipped.flat_map(|(clubdarn_series, anidb_series)| {
            anidb_series.map(|s| (s.id, clubdarn_series))
        });

        search_client.bulk_update(docs, true)?;
    }

    search_client.delete_non_clubdam()
}

fn reindex(client: &elastic::Client, path: &str) -> Result<()> {
    use std::collections::HashMap;
    use std::collections::hash_map::Entry;
    use titles::Title;

    let titles_iter = titles::TitleIterator::new(path)?;

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
        elastic::Series {
            id: id,
            titles: elastic::TitlesByLanguage::new(titles),
        }
    });

    let chunk_size = 1000;
    let should_wait = true;
    client.reindex(series, chunk_size, should_wait)
}
