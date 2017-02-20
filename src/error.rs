use std;

error_chain! {
    errors {
        InvalidId(id: String) {
            description("invalid anime ID")
            display("failed to parse anime ID as number: {}", id)
        }
        InvalidTitleType(title_type: String) {
            description("invalid title type")
            display("found unexpected title type: {}", title_type)
        }
        InvalidLine(line: String) {
            description("failed to parse input line")
            display("failed to parse input line: {}", line)
        }
    }

    foreign_links {
        Io(std::io::Error);
    }
}
