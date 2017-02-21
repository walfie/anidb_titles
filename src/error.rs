use reqwest;
use serde_json;
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
        InvalidParse(line_number: u32) {
            description("failed to parse line from file")
            display("failed to parse line {}", line_number)
        }
        InvalidUrl(url: String) {
            description("failed to parse URL")
            display("failed to parse URL {}", url)
        }
        UnexpectedResponse(url: String, resp: String) {
            description("unexpected response")
            display("unexpected response for {}\n{}", url, resp)
        }
    }

    foreign_links {
        Io(std::io::Error);
        Http(reqwest::Error);
        Json(serde_json::Error);
    }
}
