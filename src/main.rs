extern crate anidb_titles as titles;
extern crate tantivy;

use titles::error::*;

fn main() {
    let mut args = std::env::args().skip(1);
    let (path, search_term) = match (args.next(), args.next()) {
        (Some(path), Some(search_term)) => (path, search_term),
        _ => panic!("Invalid args"),
    };

    println!("{} {}", path, search_term);

    if let Err(e) = run(&path, &search_term) {
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

fn run(path: &str, search_term: &str) -> Result<()> {
    use tantivy::query::QueryParser;
    use tantivy::collector::TopCollector;

    let index = titles::index().unwrap();
    let index = titles::process_file(path, &["ja", "en", "x-jat"], index).unwrap();

    let schema = index.schema();
    let id_field = schema.get_field("id").unwrap();
    let title_field = schema.get_field("title").unwrap();
    let query_parser = QueryParser::new(index.schema(), vec![title_field]);

    let query = query_parser.parse_query(search_term).unwrap();

    let mut top_collector = TopCollector::with_limit(10);

    let searcher = index.searcher();
    query.search(&searcher, &mut top_collector);

    let doc_addresses = top_collector.docs();
    for doc_address in doc_addresses {
        println!("{:?}", doc_address);
        let retrieved_doc = searcher.doc(&doc_address).unwrap();
        println!("{}", schema.to_json(&retrieved_doc));
    }

    Ok(())
}
