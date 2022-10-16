# script to grab data from pscale and output markdown pages that are compatible with astro posts
import os
import mysql.connector
import pandas as pd
import spacy
import json
from datetime import datetime, timedelta

nlp = spacy.load("en_stonk_pipeline", disable=["lemmatizer"])

def connect_to_db():
    # read database credentials from environment variables
    db_host = os.environ.get("DB_HOST")
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASSWORD")
    db_name = os.environ.get("DB_NAME")
    return mysql.connector.connect(
        user=db_user, password=db_pass, host=db_host, database=db_name
    )


def fetch_sql_data(config: dict):
    table_name = config.get("table_name", "dp8PhLsUcFE")
    # default start date, one day ago in YYYY/MM/DD format
    one_day_ago = datetime.now() - timedelta(days=1)
    # one time fix to grab data
    if table_name == "YahooFinance":
        one_day_ago = datetime.now() - timedelta(days=15)
    default_start_date = one_day_ago.strftime("%Y/%m/%d")
    default_end_date = datetime.now().strftime("%Y/%m/%d")
    start_date = config.get("start_date", default_start_date)
    # end date is current date in YYYY-MM-DD format
    end_date = config.get("end_date", default_end_date)
    # yes this is sql injection
    # personal project, not accessible to public where video_id is a string
    query = f"SELECT * FROM {table_name} where `created_at` BETWEEN '{start_date}' AND '{end_date}'"
    # query = f"SELECT * FROM {video_id}"
    # Dont care about previous data
    new_df = pd.read_sql(query, con=connect_to_db())
    return new_df


def parse_date(date: str):
    try:
        return datetime.strptime(date, "%H:%M:%S")
    except ValueError:
        return date


def create_md_page(df: pd.DataFrame, cfg: dict):
    table_name = cfg.get("table_name", "dp8PhLsUcFE")
    page_path = f"{cfg.get('page_path')}"
    page_name = cfg.get("path_name", "index.mdx")
    segment_created_at = cfg.get("created_at", "2020-01-01")
    page_data = []
    for index, row in df.iterrows():
        # text
        text = row["text"]
        created_at = row["created_at"]
        try:
            doc = nlp(text)
            rendered_spacy = spacy.displacy.render(
                doc, style="ent", page=False)
            full_data = {
                "text": text,
                "created_at": created_at,
                "table_name": table_name,
                "spacy": rendered_spacy,
                "iteration": row["iteration"]
            }
            page_data.append(full_data)
        except Exception as e:
            print(e)
            pass

    # make pagepath if it doesnt exist
    if not os.path.exists(page_path):
        os.makedirs(page_path)
    mdx_path = f"{page_path}/{page_name}"
    # create page
    # created at
    # title
    title = f"{table_name} - {segment_created_at}"
    with open(mdx_path, "w") as f:
        page_header = f"""---
pubDate: "{segment_created_at}"
category: "{table_name}"
title: "{title}"
description: "Ornare cum cursus laoreet sagittis nunc fusce posuere per euismod dis vehicula a, semper fames lacus maecenas dictumst pulvinar neque enim non potenti. Torquent hac sociosqu eleifend potenti."
image: "../../../../assets/images/hero.jpg"
---\n"""
        f.write(page_header)
        f.write("Test Content")
        for count, item in enumerate(page_data):
            f.write(f"{item['spacy']}\n")
            if count % 5 == 0:
                # write aside to file
                fmtted_time = parse_date(item["created_at"])
                f.write(
                    f"<aside><b> Time: {fmtted_time}</b> Iteration: {item['iteration']} </aside>\n")
    return page_data

# TODO convert this to generate group of pages


def create_md_pages(config: dict):
    cts_cfg = config.get("cts", {})
    # read video_id from livestream config
    # redo this entire logic later

    # for category in cts_cfg:
    # process page here
    # TODO loop through config based on category
    categories = cts_cfg.get("categories", [])
    for category in categories:
        category_cfg = category.get("category", {})
        table_name = category.get("table_name", "dp8PhLsUcFE")
        csv_prefix = category.get("csv_prefix", "bloomberg")
        # video_id

        curr_date = datetime.now().strftime("%Y-%m-%d")
        # get current year month
        curr_year_month = datetime.now().strftime("%Y-%m")
        root_dir = f"../data/{table_name}/{curr_year_month}"

        # create folder if it exists
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)

        # current day
        curr_day = datetime.now().strftime("%d")
        csv_path = category_cfg.get("csv_path", f"{csv_prefix}_{curr_day}.csv")

        full_csv_path = f"{root_dir}/{csv_path}"

        bloom_df = fetch_sql_data({
            "table_name": table_name
        })
        bloom_df.to_csv(full_csv_path, index=False)

        # get stats for the day
        bloom_df = pd.read_csv(full_csv_path, index_col=False)
        stats_df = (pd.to_datetime(bloom_df['created_at'])
                    .dt.floor('d')
                    .value_counts()
                    .rename_axis('date')
                    .reset_index(name='count'))
        # sort by date
        stats_df = stats_df.sort_values(by=['date'])
        # drop all rows with Unnamed in the column name
        curr_parsed = 0
        page_paths = []
        base_path = "src/data/posts"

        # iterate across grouped stats df
        for index, row in stats_df.iterrows():
            # check if page exists
            # if it does, skip
            # if it doesnt, create it
            row_date = row["date"].strftime("%Y-%m-%d")
            page_name = f'{table_name}-{row_date}.md'
            page_folder = f'{table_name}'
            page_path = f"{base_path}/{page_folder}/{curr_year_month}"
            # force overwrite pages where row["date"] == current date
            # as data is still trickling in
            if os.path.exists(f"{page_path}/{page_name}") and row["date"].strftime("%Y-%m-%d") != curr_date:
                print(f"{page_path}/{page_name} exists, skipping")
                continue

            if not os.path.exists(page_path):
                print("CREATING NEW PAGE PATH")
                os.makedirs(page_path)
            # grab each chunk of data from bloomberg
            # get count
            iter_count = int(row['count'])
            end_point = curr_parsed + iter_count
            temp_df = bloom_df.iloc[curr_parsed:int(end_point)]
            curr_parsed += int(iter_count)

            create_md_page(temp_df, {
                "table_name": table_name,
                "page_path": page_path,
                "created_at": row["date"],
                "path_name": page_name
            })
            page_paths.append(f"/blog/{page_name.replace('.md', '')}")
            # append .nojekyll file to each folder
            print(f"{page_name} created")
        # create_markdown_page(chunk)
        # group csv by date in days
        # curr date in DD-MM-YYYY

        # TODO base this content on files in output folder
        with open(f"{base_path}/{table_name}/{table_name}.md", "w", encoding="utf-8") as f:
            page_header = f"""---
pubDate: "{curr_date}"
category: "video_index"
title: "{table_name} - index file"
description: "Ornare cum cursus laoreet sagittis nunc fusce posuere per euismod dis vehicula a, semper fames lacus maecenas dictumst pulvinar neque enim non potenti. Torquent hac sociosqu eleifend potenti."
image: "../../../../assets/images/hero.jpg"
---\n"""
            f.write(page_header)
            f.write("\n")
            for page in page_paths:
                f.write(f"<a href='{page}'>{page}</a>\n")

def main():
    # read config and validate env vars
    # read json file from data/mk_config.json
    with open("src/data/mk_config.json", "r") as f:
        config = json.load(f)
    create_md_pages(config)

if __name__ == "__main__":
    # for continuous livestreams, we have a post a page
    # grab all data from sql, and then split into chunks based on day
    # then output markdown pages for each day
    main()
