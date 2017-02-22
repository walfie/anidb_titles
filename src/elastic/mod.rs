use Title;
use error::*;
use itertools::Itertools;
use reqwest;
use serde_json;
use serde_json::Value as JsValue;
use time;

#[derive(Debug, Serialize)]
pub struct Series {
    pub id: u32,
    pub titles: TitlesByLanguage,
}

#[derive(Debug, Serialize)]
pub struct TitlesByLanguage(JsValue);

impl TitlesByLanguage {
    pub fn new(mut titles: Vec<Title>) -> Self {
        use serde_json::Value::{Array, String};
        use serde_json::map::{Map, Entry};

        let mut by_language = Map::new();

        titles.sort_by_key(|t| t.title_type as i8);

        while let Some(title) = titles.pop() {
            match by_language.entry(title.language) {
                Entry::Occupied(mut o) => {
                    o.get_mut().as_array_mut().unwrap().push(String(title.title));
                }
                Entry::Vacant(v) => {
                    v.insert(Array(vec![String(title.title)]));
                }
            }
        }

        TitlesByLanguage(JsValue::Object(by_language))
    }
}

pub struct Client<'a> {
    http: reqwest::Client,
    base_url: &'a str,
    alias: &'a str,
}

impl<'a> Client<'a> {
    pub fn new(base_url: &'a str, alias: &'a str) -> Result<Self> {
        Ok(Client {
            http: reqwest::Client::new()?,
            base_url: base_url,
            alias: alias,
        })
    }

    pub fn reindex<I>(&self, series: I) -> Result<()>
        where I: Iterator<Item = Series>
    {
        let now = time::now_utc();
        let now_str = now.strftime("%Y%m%d_%H%M%S").unwrap();
        let index_name = format!("{}_{}", self.alias, now_str);

        println!("Getting indexes"); // TODO: Remove
        let existing_indexes = self.get_indexes_for_alias()?;

        println!("New index"); // TODO: Remove
        self.new_index(&index_name)?;

        println!("Bulk insert"); // TODO: Remove
        self.bulk_insert(&index_name, "series", series, 250)?;

        println!("Update alias"); // TODO: Remove
        self.update_alias(index_name, &existing_indexes)
    }

    fn update_alias<T>(&self, new_index: T, old_indexes: &[T]) -> Result<()>
        where T: AsRef<str>
    {
        let actions = old_indexes.iter()
            .map(|index| ("remove", index.as_ref()))
            .chain(Some(("add", new_index.as_ref())))
            .map(|(op, index)| json!({ op: { "index": index, "alias": self.alias } }));

        let body = json!({
            "actions": actions.collect::<Vec<_>>()
        });

        let json = serde_json::to_string(&body)?;
        self.do_request(reqwest::Method::Post, "_aliases", Some(&json)).map(|_| ())
    }

    fn bulk_insert<I>(&self,
                      index_name: &str,
                      type_name: &str,
                      items: I,
                      chunk_size: usize)
                      -> Result<()>
        where I: Iterator<Item = Series>
    {
        // TODO: Items must be non-empty or this panics.
        items.chunks(chunk_size)
            .into_iter()
            .map(|chunk| {
                // TODO: Put into separate function, also multithread
                let mut batch = chunk.flat_map(|series| {
                        let action = json!({
                            "index": {
                                "_index": index_name,
                                "_type": type_name,
                                "_id": series.id
                            }
                        });

                        // TODO: Don't unwrap. Also don't use vec?
                        vec![
                            serde_json::to_string(&action).unwrap(),
                            serde_json::to_string(&series).unwrap()
                        ]
                    })
                    .join("\n");

                batch.push('\n');

                self.do_request(reqwest::Method::Put, "_bulk", Some(&batch)).map(|_| ())
            })
            .fold_results((), |_, _| ())
    }

    fn get_indexes_for_alias(&self) -> Result<Vec<String>> {
        let mut result = self.do_request(reqwest::Method::Get, "_aliases", None)?;

        let json = result.json::<JsValue>()?;

        if let JsValue::Object(obj) = json {
            Ok(obj.keys().cloned().collect())
        } else {
            Err(format!("expected JSON object, got {}", json))?
        }
    }

    fn new_index(&self, index_name: &str) -> Result<()> {
        let json = serde_json::to_string(&mappings())?;
        self.do_request(reqwest::Method::Put, index_name, Some(&json)).map(|_| ())
    }

    fn do_request(&self,
                  method: reqwest::Method,
                  path: &'a str,
                  body: Option<&'a str>)
                  -> Result<reqwest::Response> {
        let url_str = format!("{}/{}", self.base_url, path);

        let url =
            reqwest::Url::parse(&url_str).chain_err(|| ErrorKind::InvalidUrl(url_str.clone()))?;

        let auth = if !url.username().is_empty() || !url.password().is_none() {
            use reqwest::header::{Authorization, Basic};

            Some(Authorization(Basic {
                username: url.username().to_string(),
                password: url.password().map(|p| p.to_string()),
            }))
        } else {
            None
        };

        let mut req = self.http.request(method, url);

        if let Some(b) = body {
            req = req.body(b);
        }

        if let Some(a) = auth {
            req = req.header(a);
        }

        let mut response = req.send()?;

        if response.status().is_success() {
            Ok(response)
        } else {
            use std::io::Read;

            let mut response_str = String::new();
            response.read_to_string(&mut response_str)?;
            Err(ErrorKind::UnexpectedResponse(url_str, response_str).into())
        }
    }
}

fn mappings() -> serde_json::Value {
    json!({
        "mappings": {
            "series": {
                "_all": { "enabled": false },
                "properties": {
                    "titles": {
                        "properties": {
                            "x_jat": { "analyzer": "standard" },
                            "ja": { "analyzer": "cjk" },
                            "en": { "analyzer": "english" }
                        }
                    }
                }
            }
        }
    })
}
