extern crate anidb_titles as titles;

use titles::error::*;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = match args.next() {
        Some(path) => path,
        _ => panic!("Invalid args"),
    };

    println!("{}", path);

    if let Err(e) = run(&path) {
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

fn run(path: &str) -> Result<()> {
    let titles_iter = titles::TitleIterator::new(path, &["ja", "en", "x-jat"])?;

    use std::collections::HashMap;
    use std::collections::hash_map::Entry;
    use titles::Title;

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

    for (id, titles) in titles_hash_map {
        println!("{}", id);
        for title in titles {
            println!("    {} ({} {:?})",
                     title.title,
                     title.language,
                     title.title_type);
        }
    }
    Ok(())
}
