use Title;
use clubdarn;
use error::*;
use itertools::Itertools;
use reqwest;
use reqwest::Method;
use serde_json;
use serde_json::Value as JsValue;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use time;


#[derive(Debug, Deserialize, Serialize)]
pub struct Series {
    pub id: u32,
    pub titles: TitlesByLanguage,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TitlesByLanguage(HashMap<String, Vec<String>>);

impl TitlesByLanguage {
    pub fn new(mut titles: Vec<Title>) -> Self {
        let mut by_language: HashMap<String, Vec<String>> = HashMap::new();

        titles.sort_by_key(|t| t.title_type as i8);

        while let Some(title) = titles.pop() {
            match by_language.entry(title.language) {
                Entry::Occupied(mut o) => {
                    o.get_mut().push(title.title);
                }
                Entry::Vacant(v) => {
                    v.insert(vec![title.title]);
                }
            }
        }

        TitlesByLanguage(by_language)
    }
}

pub struct Client<'a> {
    http: reqwest::Client,
    base_url: &'a str,
    alias: &'a str,
    type_name: &'a str,
}

impl<'a> Client<'a> {
    pub fn new(base_url: &'a str, alias: &'a str, type_name: &'a str) -> Result<Self> {
        Ok(Client {
            http: reqwest::Client::new()?,
            base_url: base_url,
            alias: alias,
            type_name: type_name,
        })
    }

    pub fn reindex<I>(&self, series: I, chunk_size: usize, should_wait: bool) -> Result<Vec<String>>
        where I: IntoIterator<Item = Series>
    {
        let now = time::now_utc();
        let now_str = now.strftime("%Y%m%d_%H%M%S").unwrap();
        let index_name = format!("{}_{}", self.alias, now_str);

        println!("Getting indices"); // TODO: Remove
        let existing_indexes = self.get_indexes_for_alias()?;

        println!("New index"); // TODO: Remove
        self.new_index(&index_name)?;

        println!("Bulk insert"); // TODO: Remove
        for chunk in &series.into_iter().chunks(chunk_size) {
            self.bulk_insert(&index_name, chunk, should_wait)?;
        }

        println!("Update alias"); // TODO: Remove
        self.update_alias(index_name, &existing_indexes)?;

        Ok(existing_indexes)
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
        self.do_request(Method::Post, "_aliases", Some(&json)).map(|_| ())
    }

    pub fn delete_indices<T>(&self, indices: &[T]) -> Result<()>
        where T: AsRef<str>
    {
        indices.iter()
            .map(|index| self.do_request(Method::Delete, index.as_ref(), None))
            .fold_results((), |_, _| ())
    }

    fn bulk_insert<I>(&self, index_name: &str, items: I, should_wait: bool) -> Result<()>
        where I: IntoIterator<Item = Series>
    {
        let mut body = items.into_iter()
            .map(|series| {
                let action = json!({ "index": { "_id": series.id } });

                format!(
                    "{}\n{}",
                    serde_json::to_string(&action).unwrap(),
                    serde_json::to_string(&series).unwrap()
                )
            })
            .join("\n");

        body.push('\n');

        let wait_for = if should_wait { "?refresh=wait_for" } else { "" };

        self.do_request(Method::Put,
                        &format!("{}/{}/_bulk{}", index_name, self.type_name, wait_for),
                        Some(&body))
            .map(|_| ())
    }

    pub fn delete_non_clubdam(&self) -> Result<()> {
        let page_size = 500;
        let query = json!({
            "query": {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "clubdam"
                        }
                    }
                }
            },
            "sort": ["_doc"],
            "fields": [],
            "size": page_size
        });

        let ids_iter = ScrollSearch {
            client: self,
            query: query,
            scroll_id: None,
        };

        ids_iter.map(|ids| {
                let mut body = ids?
                        .into_iter()
                        .map(|id| {
                            let delete = json!({"delete": { "_id": id }});
                            serde_json::to_string(&delete).unwrap()
                        })
                        .join("\n");

                body.push('\n');

                self.do_request(Method::Put,
                                &format!("{}/{}/_bulk", self.alias, self.type_name),
                                Some(&body))
            })
            .fold_results((), |_, _| ())
    }

    pub fn bulk_update<I>(&self, items: I, should_wait: bool) -> Result<()>
        where I: IntoIterator<Item = (u32, clubdarn::Series)>
    {
        let mut body = items.into_iter()
            .map(|(id, series)| {
                let action = json!({ "update": { "_id": id } });
                let doc = json!({ "doc": { "clubdam": series } });

                format!(
                    "{}\n{}",
                    serde_json::to_string(&action).unwrap(),
                    serde_json::to_string(&doc).unwrap()
                )
            })
            .join("\n");

        body.push('\n');

        let wait_for = if should_wait { "?refresh=wait_for" } else { "" };

        self.do_request(Method::Put,
                        &format!("{}/{}/_bulk{}", self.alias, self.type_name, wait_for),
                        Some(&body))
            .map(|_| ())
    }

    fn get_indexes_for_alias(&self) -> Result<Vec<String>> {
        let mut result = self.do_request(Method::Get, "_aliases", None)?;

        let json = result.json::<JsValue>()?;

        if let JsValue::Object(obj) = json {
            Ok(obj.keys().cloned().collect())
        } else {
            Err(format!("expected JSON object, got {}", json))?
        }
    }

    fn new_index(&self, index_name: &str) -> Result<()> {
        let json = serde_json::to_string(&mappings())?;
        self.do_request(Method::Put, index_name, Some(&json)).map(|_| ())
    }

    // TODO: Make this type signature not terrible
    pub fn multi_search<T, L, S1, S2>(&self, titles: T, languages: L) -> Result<Vec<Option<Series>>>
        where T: IntoIterator<Item = S1>,
              S1: AsRef<str>,
              L: IntoIterator<Item = S2>,
              S2: AsRef<str>
    {
        let fields =
            languages.into_iter().map(|l| format!("titles.{}", l.as_ref())).collect::<Vec<_>>();

        let mut requests = titles.into_iter()
            .map(|title| {
                let query = json!({
                    "size": 1,
                    "query": {
                        "multi_match": {
                            "query": title.as_ref(),
                            "fields": fields
                        }
                    }
                });

                format!("{{}}\n{}", serde_json::to_string(&query).unwrap())
            })
            .join("\n");

        requests.push('\n');

        let mut result = self.do_request(Method::Post,
                        &format!("{}/_msearch", self.alias),
                        Some(&requests))?
            .json::<JsValue>()?;

        let mut empty_vec = Vec::new();
        let series = result.get_mut("responses")
            .and_then(|r| r.as_array_mut())
            .unwrap_or(&mut empty_vec)
            .iter_mut()
            .map(|json| {
                json.pointer_mut("/hits/hits/0/_source").and_then(|s| {
                    let source = ::std::mem::replace(s, JsValue::Null);
                    serde_json::from_value::<Series>(source).ok() // TODO: Use Result
                })
            });

        Ok(series.collect::<Vec<_>>())
    }

    fn do_request(&self,
                  method: Method,
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

pub struct ScrollSearch<'a> {
    client: &'a Client<'a>,
    query: JsValue,
    scroll_id: Option<String>,
}

impl<'a> Iterator for ScrollSearch<'a> {
    type Item = Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut get_response = || {
            let mut response = if let Some(ref scroll) = self.scroll_id {
                    let q = json!({ "scroll": "1m", "scroll_id": scroll });
                    let body = serde_json::to_string(&q)?;

                    self.client.do_request(Method::Post, "_search/scroll", Some(&body))
                } else {
                    let body = serde_json::to_string(&self.query)?;

                    self.client.do_request(Method::Post,
                                           &format!("{}/_search?scroll=1m", self.client.alias),
                                           Some(&body))
                }?
                .json::<JsValue>()?;

            let mut empty_vec = Vec::new();

            self.scroll_id =
                response.get("_scroll_id").and_then(|id| id.as_str()).map(|id| id.to_string());

            let hits = response.pointer_mut("/hits/hits")
                .and_then(|r| r.as_array_mut())
                .unwrap_or(&mut empty_vec);

            if hits.is_empty() {
                Ok(None)
            } else {
                let ids_str = hits.into_iter()
                    .flat_map(|hit| {
                        hit.get("_id").and_then(|id| id.as_str()).map(|id| id.to_string())
                    })
                    .collect::<Vec<String>>();
                Ok(Some(ids_str))
            }
        };

        match get_response() {
            Ok(Some(resp)) => Some(Ok(resp)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

fn mappings() -> serde_json::Value {
    json!({
        "mappings": {
            "series": {
                "_all": { "enabled": false },
                "properties": {
                    "clubdam": {
                        "properties": {
                            "title": {
                                "type": "string",
                                "analyzer": "cjk"
                            }
                        }
                    },
                    "titles": {
                        "properties": {
                            "x-jat": {
                                "type": "string",
                                "analyzer": "standard"
                            },
                            "ja": {
                                "type": "string",
                                "analyzer": "cjk"
                            },
                            "en": {
                                "type": "string",
                                "analyzer": "english"
                            }
                        }
                    }
                }
            }
        }
    })
}
