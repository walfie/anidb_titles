use std;

error_chain! {
    errors {
        InvalidId(id: String, line_number: u32) {
            description("invalid anime ID")
            display("failed to parse anime ID \"{}\" as number on line {}", id, line_number)
        }
        InvalidTitleType(title_type: String) {
            description("invalid title type")
            display("found unexpected title type: {}", title_type)
        }
        InvalidLine(line: String) {
            description("failed to parse input line")
            display("failed to parse input line: {}", line)
        }
        InvalidParse(line_number: u32) {
            description("failed to parse line from file")
            display("failed to parse line {}", line_number)
        }
    }

    foreign_links {
        Io(std::io::Error);
    }
}
