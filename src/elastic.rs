use Title;

#[derive(Debug, Serialize)]
pub struct Series {
    pub id: u32,
    pub titles: TitlesByLanguage,
}

#[derive(Debug, Serialize)]
pub struct TitlesByLanguage {
    pub x_jat: Vec<String>,
    pub ja: Vec<String>,
    pub en: Vec<String>,
}

impl TitlesByLanguage {
    pub fn new(mut titles: Vec<Title>) -> Self {
        let mut by_language = TitlesByLanguage {
            x_jat: vec![],
            ja: vec![],
            en: vec![],
        };

        titles.sort_by_key(|t| t.title_type as i8);

        while let Some(title) = titles.pop() {
            match title.language.as_ref() {
                "x-jat" => by_language.x_jat.push(title.title),
                "ja" => by_language.ja.push(title.title),
                "en" => by_language.en.push(title.title),
                _ => (),
            }
        }

        by_language
    }
}
