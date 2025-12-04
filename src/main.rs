use std::{collections::HashMap, env::var, fs::File, io::Write, path::Path, time::Duration};

use anyhow::{Result, bail};
use chrono::Datelike;
use clap::Parser;
use polars::prelude::*;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

#[derive(Debug, clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Scrape {
        /// The year you want to summarize, defaults to current year
        #[arg(short = 'y', long = "year")]
        year: Option<i32>,
        /// The page to scrape from
        #[arg(default_value = "readings")]
        scrape_type: String,
        /// Delay between page loads
        #[arg(short = 'd', default_value = "6000")]
        delay_ms: u64,
    },
    StatsOnly {
        /// The year to load
        year: i32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (df, stats) = match args.command {
        Command::Scrape {
            year,
            scrape_type,
            delay_ms,
        } => {
            let year = year.unwrap_or_else(|| chrono::Local::now().year());

            let client = reqwest::ClientBuilder::new()
                .user_agent("AO3Wrapped/1.0.0")
                .cookie_store(true)
                .redirect(reqwest::redirect::Policy::default())
                .build()
                .unwrap();

            println!("Getting CSRF token...");
            let csrf = get_csrf(&client).await?;
            sleep(Duration::from_secs(2)).await;
            println!("Logging in...");
            let username = sign_in(&client, &csrf).await?;
            println!("Logged in as {username}");

            let mut page = 1;
            let mut stats = Stats::default();
            let mut df = DataFrame::empty();
            loop {
                println!("Fetching page {page}...");
                let url = format!(
                    "https://archiveofourown.org/users/{username}/{scrape_type}?page={page}"
                );

                let res = loop {
                    match client.get(&url).send().await?.error_for_status() {
                        Ok(r) => break r.text().await?,
                        Err(e) => {
                            eprintln!("Failed to fetch page {page}: {e}");
                        }
                    }
                };

                println!("Processing page...");
                let doc = Html::parse_document(&res);

                if !parse_hist_page(&doc, &format!("{year}"), &mut stats, &mut df)? {
                    break;
                }

                page += 1;

                println!("Waiting {} ms...", delay_ms);
                sleep(Duration::from_millis(delay_ms)).await;
            }

            std::fs::write(
                format!("user_{year}.json"),
                serde_json::to_string_pretty(&stats)?,
            )?;
            CsvWriter::new(File::create(format!("works_{year}.csv"))?).finish(&mut df)?;

            (df, stats)
        }
        Command::StatsOnly { year } => {
            if !Path::new(&format!("user_{year}.json")).exists() {
                bail!("User stats file not found");
            } else if !Path::new(&format!("works_{year}.csv")).exists() {
                bail!("Works file not found");
            }

            let df = CsvReader::new(File::open(format!("works_{year}.csv"))?).finish()?;
            let stats =
                serde_json::from_str(&std::fs::read_to_string(format!("user_{year}.json"))?)?;

            (df, stats)
        }
    };

    print_stats(&df.lazy(), &stats)?;

    Ok(())
}

fn print_stats(df: &LazyFrame, stats: &Stats) -> Result<()> {
    println!(
        "You've read {} fanfics this year, totaling {} words, or {:.2} words/day. There's about 70000 words in a novel. You could've read {:.2} novels this year, but you read fanfics instead.",
        df.clone().collect()?.height(),
        stats.user_word_count,
        stats.user_word_count as f32 / 365.0,
        stats.user_word_count as f32 / 70000.0
    );

    println!();

    let most_visited = df
        .clone()
        .select([col("user_visitations"), col("title"), col("authors")])
        .filter(col("user_visitations").eq(col("user_visitations").max()))
        .collect()?;

    println!(
        "The fic you've visited the most was {} by {}, with {} visits.",
        most_visited.column("title")?.get(0)?.str_value(),
        most_visited.column("authors")?.get(0)?.str_value(),
        most_visited.column("user_visitations")?.get(0)?.str_value()
    );

    println!();

    const RUNNERS_UP: usize = 9;

    fn print_top_and_rest<T>(
        map: &HashMap<String, T>,
        intro: &str,
        also_intro: &str,
        format_fn: impl Fn(T, &str) -> String,
    ) where
        T: Ord + Copy + std::fmt::Display,
    {
        let mut sorted: Vec<_> = map.iter().collect();
        sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));

        if let Some((top_key, top_val)) = sorted.first() {
            println!(
                "{}",
                intro
                    .replace("{}", &top_val.to_string())
                    .replace("{key}", top_key)
            );
            if sorted.len() > 1 {
                println!("{}", also_intro);
                for (key, val) in sorted.iter().skip(1).take(RUNNERS_UP) {
                    println!("{}", format_fn(**val, key));
                }
            }
            println!();
        }
    }

    // Ship type stats
    print_top_and_rest(
        &stats.user_ship_type,
        "You read {} {key} fics this year.",
        "You also read",
        |val, key| format!("{} {} fics", val, key),
    );

    // Rating stats
    print_top_and_rest(
        &stats.user_rating,
        "You read {} {key} fics this year.",
        "You also read",
        |val, key| format!("{} {} fics", val, key),
    );

    // Status stats
    let mut status_sorted: Vec<_> = stats.user_status.iter().collect();
    status_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if status_sorted.len() >= 2 {
        let (key0, val0) = status_sorted[0];
        let (key1, val1) = status_sorted[1];
        println!(
            "You read {} {} and {} {} fics this year.",
            val0, key0, val1, key1
        );
        println!();
    }

    // Authors stats
    let mut authors_sorted: Vec<_> = stats.user_authors.iter().collect();
    authors_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if let Some((top_key, top_val)) = authors_sorted.first() {
        println!(
            "You read {} different authors this year.",
            stats.user_authors.len()
        );
        println!(
            "Your most read author this year was {}, with {} fics.",
            top_key, top_val
        );
        println!("You also read:");
        for (key, val) in authors_sorted.iter().skip(1).take(RUNNERS_UP) {
            println!("{} fics by {}", val, key);
        }
        println!();
    }

    // Fandoms stats
    let mut fandoms_sorted: Vec<_> = stats.user_fandoms.iter().collect();
    fandoms_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if let Some((top_key, top_val)) = fandoms_sorted.first() {
        println!(
            "You read fics for {} different fandoms this year.",
            stats.user_fandoms.len()
        );
        println!(
            "Your most read fandom was {}, with {} fics this year.",
            top_key, top_val
        );
        println!("You also read:");
        for (key, val) in fandoms_sorted.iter().skip(1).take(RUNNERS_UP) {
            println!("{} {} fics", val, key);
        }
        println!();
    }

    // Ships stats
    let mut ships_sorted: Vec<_> = stats.user_ships.iter().collect();
    ships_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if let Some((top_key, top_val)) = ships_sorted.first() {
        println!(
            "You read fics with {} different ships this year.",
            stats.user_ships.len()
        );
        println!(
            "Are you not tired of reading about {}? You read {} fics of them this year.",
            top_key, top_val
        );
        println!("You also read:");
        for (key, val) in ships_sorted.iter().skip(1).take(RUNNERS_UP) {
            println!("{} {} fics", val, key);
        }
        println!();
    }

    // Characters stats
    let mut characters_sorted: Vec<_> = stats.user_characters.iter().collect();
    characters_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if let Some((top_key, top_val)) = characters_sorted.first() {
        println!(
            "You read about {} different characters this year.",
            stats.user_characters.len()
        );
        println!(
            "What a {} stan. You read {} fics of them this year.",
            top_key, top_val
        );
        println!("You also read:");
        for (key, val) in characters_sorted.iter().skip(1).take(RUNNERS_UP) {
            println!("{} {} fics", val, key);
        }
        println!();
    }

    // Tags stats
    let mut tags_sorted: Vec<_> = stats.user_tags.iter().collect();
    tags_sorted.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    if let Some((top_key, top_val)) = tags_sorted.first() {
        let df_height = df.clone().collect()?.height();
        println!(
            "You read fics with {} different tags this year, averaging {:.2} tags/work.",
            stats.user_tags.len(),
            stats.user_tags.len() as f32 / df_height as f32
        );
        println!(
            "You absolutely love {}, but you already knew that. You read {} fics with that tag this year.",
            top_key, top_val
        );
        println!("You also read:");
        for (key, val) in tags_sorted.iter().skip(1).take(RUNNERS_UP) {
            println!("{} {} fics", val, key);
        }
    }

    println!();

    print_min_max_stats(df)?;

    Ok(())
}

fn print_min_max_stats(df: &LazyFrame) -> Result<()> {
    fn print_stat(df: &LazyFrame, col_name: &str, label: &str, is_max: bool) -> Result<()> {
        let filtered = df
            .clone()
            .filter(if is_max {
                col(col_name).eq(col(col_name).max())
            } else {
                col(col_name).eq(col(col_name).min())
            })
            .select([col("title"), col("authors"), col(col_name)])
            .collect()?;

        let extremum = if is_max { "Most" } else { "Least" };
        println!(
            "{} {}: {} by {} with {} {}",
            extremum,
            label,
            filtered.column("title")?.get(0)?.str_value(),
            filtered.column("authors")?.get(0)?.str_value(),
            filtered.column(col_name)?.get(0)?,
            label
        );
        Ok(())
    }

    // Word count stats
    print_stat(df, "word_count", "word count", true)?;
    print_stat(df, "word_count", "word count", false)?;
    let mean_words = df
        .clone()
        .select([col("word_count").mean()])
        .collect()?
        .column("word_count")?
        .get(0)?
        .extract::<f64>()
        .unwrap_or_default();
    println!("Average word count: {}", mean_words as i64);
    println!();

    // Hits stats
    print_stat(df, "hits", "hits", true)?;
    print_stat(df, "hits", "hits", false)?;
    let mean_hits = df
        .clone()
        .select([col("hits").mean()])
        .collect()?
        .column("hits")?
        .get(0)?
        .extract::<f64>()
        .unwrap_or_default();
    println!("Average hits: {}", mean_hits as i64);
    println!();

    // Kudos stats
    print_stat(df, "kudos", "kudos", true)?;
    print_stat(df, "kudos", "kudos", false)?;
    let mean_kudos = df
        .clone()
        .select([col("kudos").mean()])
        .collect()?
        .column("kudos")?
        .get(0)?
        .extract::<f64>()
        .unwrap_or_default();
    println!("Average kudos: {}", mean_kudos as i64);

    Ok(())
}

fn selector(s: impl AsRef<str>) -> Selector {
    Selector::parse(s.as_ref()).unwrap()
}

async fn get_csrf(client: &reqwest::Client) -> Result<String> {
    let res = client
        .get("https://archiveofourown.org/users/login")
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let doc = scraper::Html::parse_document(&res);
    Ok(doc
        .select(&selector("meta[name=\"csrf-token\"]"))
        .next()
        .unwrap()
        .attr("content")
        .unwrap()
        .to_string())
}

async fn sign_in(client: &reqwest::Client, csrf: &str) -> Result<String> {
    let username = var("AO3_USERNAME").unwrap_or_else(|_| prompt("Enter your username: ", false));
    let password = var("AO3_PASSWORD").unwrap_or_else(|_| prompt("Enter your password: ", true));

    let params = [
        ("utf8", "✓"),
        ("authenticity_token", csrf),
        ("user[login]", &username),
        ("user[password]", &password),
        ("commit", "Log in"),
    ]
    .into_iter()
    .collect::<HashMap<&str, &str>>();

    client
        .post("https://archiveofourown.org/users/login")
        .header("Referer", "https://archiveofourown.org/users/login")
        .header("Origin", "https://archiveofourown.org")
        .form(&params)
        .send()
        .await?
        .error_for_status()?;

    Ok(username)
}

fn prompt(p: &str, secure: bool) -> String {
    if secure {
        loop {
            match rpassword::prompt_password(p) {
                Ok(pass) => return pass,
                Err(_) => eprintln!("Invalid password"),
            }
        }
    } else {
        loop {
            print!("{p}");
            let _ = std::io::stdout().flush();
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).unwrap();
            line.trim().to_string();
            if line.is_empty() {
                continue;
            }
            return line;
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Stats {
    user_authors: HashMap<String, u32>,
    user_fandoms: HashMap<String, u32>,
    user_ship_type: HashMap<String, u32>,
    user_rating: HashMap<String, u32>,
    user_status: HashMap<String, u32>,
    user_ships: HashMap<String, u32>,
    user_characters: HashMap<String, u32>,
    user_tags: HashMap<String, u32>,
    user_word_count: u64,
    title_lower_count: u32,
}

fn parse_hist_page(html: &Html, year: &str, stats: &mut Stats, df: &mut DataFrame) -> Result<bool> {
    let work_list_sel =
        Selector::parse("ol.reading.work.index.group li[class*='reading work blurb group']")
            .unwrap();
    let user_module_sel = Selector::parse("div.user.module.group h4").unwrap();
    let header_sel = Selector::parse("div.header.module").unwrap();
    let title_sel = Selector::parse("h4.heading a").unwrap();
    let author_sel = Selector::parse("h4.heading a[rel='author']").unwrap();
    let date_sel = Selector::parse("p").unwrap();
    let fandom_sel = Selector::parse("h5.fandoms.heading a").unwrap();
    let req_tag_sel = Selector::parse("ul li a span.text").unwrap();
    let ship_sel = Selector::parse("ul.tags.commas li.relationships").unwrap();
    let char_sel = Selector::parse("ul.tags.commas li.characters").unwrap();
    let tag_sel = Selector::parse("ul.tags.commas li.freeforms").unwrap();
    let stats_sel = Selector::parse("dl.stats").unwrap();
    let words_sel = Selector::parse("dd.words").unwrap();
    let kudos_sel = Selector::parse("dd.kudos a").unwrap();
    let hits_sel = Selector::parse("dd.hits").unwrap();

    let mut found_in_year = false;

    for work in html.select(&work_list_sel) {
        // Get last visited date
        let Some(user_module) = work.select(&user_module_sel).next() else {
            continue;
        };
        let last_visited_text = user_module.text().collect::<String>();
        let last_visited = last_visited_text
            .trim()
            .strip_prefix("Last visited:")
            .unwrap_or("")
            .lines()
            .next()
            .unwrap_or("")
            .trim();

        if !last_visited.contains(year) {
            continue;
        }

        found_in_year = true;

        // Get title
        let Some(header) = work.select(&header_sel).next() else {
            continue;
        };
        let Some(title_elem) = header.select(&title_sel).next() else {
            continue;
        };
        let title = title_elem.text().collect::<String>();

        if title == title.to_lowercase() {
            stats.title_lower_count += 1;
        }

        // Get authors
        let mut authors = Vec::new();
        for author in header.select(&author_sel) {
            let author_text = author.text().collect::<String>();
            if author_text != "orphan_account" {
                authors.push(author_text.clone());
                *stats.user_authors.entry(author_text).or_insert(0) += 1;
            }
        }

        // Get date last updated
        let updated = header
            .select(&date_sel)
            .next()
            .map(|e| e.text().collect::<String>())
            .unwrap_or_default();

        // Get fandoms
        let mut fandoms = Vec::new();
        for fandom in header.select(&fandom_sel) {
            let fandom_text = fandom.text().collect::<String>();
            fandoms.push(fandom_text.clone());
            *stats.user_fandoms.entry(fandom_text).or_insert(0) += 1;
        }

        // Get required tags (rating, ship types, status)
        let req_tags: Vec<String> = header
            .select(&req_tag_sel)
            .map(|t| t.text().collect::<String>())
            .collect();

        if req_tags.len() < 4 {
            continue;
        }

        let rating = req_tags[0].clone();
        *stats.user_rating.entry(rating.clone()).or_insert(0) += 1;

        let mut ship_types = Vec::new();
        for ship_type in req_tags[2].split(", ") {
            ship_types.push(ship_type.to_string());
            *stats
                .user_ship_type
                .entry(ship_type.to_string())
                .or_insert(0) += 1;
        }

        let work_status = req_tags[3].clone();
        *stats.user_status.entry(work_status.clone()).or_insert(0) += 1;

        // Get relationships
        let mut ships = Vec::new();
        for ship in work.select(&ship_sel) {
            let ship_text = ship.text().collect::<String>();
            ships.push(ship_text.clone());
            *stats.user_ships.entry(ship_text).or_insert(0) += 1;
        }

        // Get characters
        let mut characters = Vec::new();
        for character in work.select(&char_sel) {
            let char_text = character.text().collect::<String>();
            characters.push(char_text.clone());
            *stats.user_characters.entry(char_text).or_insert(0) += 1;
        }

        // Get additional tags
        let mut additional_tags = Vec::new();
        for tag in work.select(&tag_sel) {
            let tag_text = tag.text().collect::<String>();
            additional_tags.push(tag_text.clone());
            *stats.user_tags.entry(tag_text).or_insert(0) += 1;
        }

        // Get stats
        let Some(stats_elem) = work.select(&stats_sel).next() else {
            continue;
        };

        let word_count = stats_elem
            .select(&words_sel)
            .next()
            .and_then(|e| e.text().collect::<String>().replace(",", "").parse().ok())
            .unwrap_or(0);
        stats.user_word_count += word_count;

        let kudos = stats_elem
            .select(&kudos_sel)
            .next()
            .and_then(|e| e.text().collect::<String>().replace(",", "").parse().ok())
            .unwrap_or(0);

        let hits = stats_elem
            .select(&hits_sel)
            .next()
            .and_then(|e| e.text().collect::<String>().replace(",", "").parse().ok())
            .unwrap_or(0);

        // Get visitations
        let visitations_text = last_visited_text
            .split("Visited ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .unwrap_or("once");

        let user_visitations = if visitations_text == "once" {
            1
        } else {
            visitations_text.parse().unwrap_or(1)
        };

        *df = df.vstack(&df![
            "title" => [title.as_str()],
            "authors" => [authors.join(",")],
            "last_updated" => [updated.as_str()],
            "fandoms" => [fandoms.join(",")],
            "characters" => [characters.join(",")],
            "ship_types" => [ship_types.join(",")],
            "rating" => [rating.as_str()],
            "work_stats" => [work_status.as_str()],
            "ships" => [ships.join(",")],
            "additional_tags" => [additional_tags.join(",")],
            "word_count" => [word_count],
            "kudos" => [kudos],
            "hits" => [hits],
            "user_last_visited" => [last_visited],
            "user_visitations" => [user_visitations]
        ]?)?;
    }

    Ok(found_in_year)
}
