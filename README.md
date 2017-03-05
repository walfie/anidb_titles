# ClubDAM AniDB indexer

This tool indexes ClubDAM anime series titles in Elasticsearch, using
AniDB's anime titles archive to provide supplemental search terms.

ClubDAM only has titles in Japanese, but AniDB provides titles in
multiple languages. Combining the two allows us to search for series on
ClubDAM using English titles, for example.

This tool works as follows:

1. Parses AniDB's anime titles archive, and combines titles by series ID.

2. Inserts series into Elasticsearch, in the `series` index.
   Documents look like:

   ```json
   {
     "_id": "9348",
     "main_title": "アイカツ! アイドルカツドウ!"
     "titles": {
       "x-jat":["Aikatsu! Idol Katsudou!"],
       "en":["Aikatsu! Idol Activities!"],
       "ja":["アイカツ! アイドルカツドウ!"]
     }
   }
   ```

   Titles are sorted in the order: Primary, Official, Synonym, Short
   (as indicated in the AniDB archive)

3. Gets all anime series from ClubDAM, and attempts to find an existing
   title in Elasticsearch. Exact matches in `main_title` are
   prioritized.

4. We add an additional field `titles.clubdam` to the matching
   documents, containing the ClubDAM title. Using the example above:

   ```diff
    {
      "_id": "9348",
      "main_title": "アイカツ! アイドルカツドウ!"
      "titles": {
        "x-jat":["Aikatsu! Idol Katsudou!"],
        "en":["Aikatsu! Idol Activities!"],
        "ja":["アイカツ! アイドルカツドウ!"]
   +    "clubdam": ["アイカツ"]
      }
    }
   ```

5. Finally, delete all documents that don't have a ClubDAM title.

## Download AniDB archive

AniDB provides a daily updated dump of its anime titles. Do not request this
file more than once a day (see the
[wiki](https://wiki.anidb.net/w/API#Anime_Titles)).

```sh
wget http://anidb.net/api/anime-titles.dat.gz
gunzip <anime-titles.dat.gz >anime-titles.dat
```
## Build

```sh
cargo build
```

## Run

```sh
export ELASTICSEARCH_URL=http://localhost:9200
./target/debug/clubdam_anidb_indexer anime-titles.dat $ELASTICSEARCH_URL
```

