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
    let hashmap = titles::process_file(path, &["ja", "en", "x-jat"])?;
    for (id, titles) in &hashmap {
        println!("{}", id);
        for title in titles {
            println!("    {} ({})", title.title, title.language);
        }
    }
    Ok(())
}
