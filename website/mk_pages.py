# script to grab data from pscale and output markdown pages that are compatible with astro posts
import os 
import mysql.connector
import pandas as pd
import spacy
from datetime import datetime, timedelta

nlp = spacy.load("en_core_web_sm")

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
    video_id = config.get("video_id", "dp8PhLsUcFE")
    # default start date, one day ago in YYYY/MM/DD format
    one_day_ago = datetime.now() - timedelta(days=1)
    default_start_date = one_day_ago.strftime("%Y/%m/%d")
    default_end_date = datetime.now().strftime("%Y/%m/%d")
    start_date = config.get("start_date", default_start_date)
    # end date is current date in YYYY-MM-DD format
    end_date = config.get("end_date", default_end_date)
    # yes this is sql injection
    # personal project, not accessible to public where video_id is a string
    query = f"SELECT * FROM {video_id} where `created_at` BETWEEN '{start_date}' AND '{end_date}'"
    # query = f"SELECT * FROM {video_id}"
    # Dont care about previous data
    new_df = pd.read_sql(query, con=connect_to_db())
    return new_df

def map_video_id_to_title(video_id: str):
    # map video_id to title
    return "Bloomberg Livestream"

def parse_date(date: str):
    try:
        return datetime.strptime(date, "%H:%M:%S")
    except ValueError:
        return date

def create_md_page(df: pd.DataFrame, cfg: dict):
    video_id = cfg.get("video_id", "dp8PhLsUcFE")
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
            rendered_spacy = spacy.displacy.render(doc, style="ent", page=False)
            full_data = {
                "text": text,
                "created_at": created_at,
                "video_id": video_id,
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
    title = f"{map_video_id_to_title(video_id)} - {segment_created_at}"
    with open(mdx_path, "w") as f:
        page_header = f"""---
pubDate: "{segment_created_at}"
category: "{video_id}"
title: "{title}"
description: "Ornare cum cursus laoreet sagittis nunc fusce posuere per euismod dis vehicula a, semper fames lacus maecenas dictumst pulvinar neque enim non potenti. Torquent hac sociosqu eleifend potenti."
image: "~/assets/images/hero.jpg"
---\n"""
        f.write(page_header) 
        for count, item in enumerate(page_data):
            f.write(f"{item['spacy']}\n")
            if count % 5 == 0:
                # write aside to file
                fmtted_time = parse_date(item["created_at"])
                f.write(f"<aside><b> Time: {fmtted_time}</b> Iteration: {item['iteration']} </aside>\n")   
    return page_data

# TODO convert this to generate group of pages
def create_md_pages(config: dict):
    cts_cfg = config.get("cts", {})
    # read video_id from livestream config
    # redo this entire logic later

    # for category in cts_cfg:
    # process page here
    # TODO loop through config based on category
    category_cfg = cts_cfg.get("category", {})
    video_id = category_cfg.get("video_id", "dp8PhLsUcFE")
    # video_id

    curr_date = datetime.now().strftime("%Y-%m-%d")
    # get current year month
    curr_year_month = datetime.now().strftime("%Y-%m")
    root_dir = f"../data/{video_id}/{curr_year_month}"

    # create folder if it exists
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

    # current day
    curr_day = datetime.now().strftime("%d")
    csv_path = category_cfg.get("csv_path", f"bloomberg_{curr_day}.csv")

    full_csv_path = f"{root_dir}/{csv_path}"
    # if bloomberg.csv exists, skip grabbing it
    # if os.path.exists(csv_path):
    #     print("Bloomberg.csv exists, skipping fetch")
    # else:
    # read video_id from livestream config
    bloom_df = fetch_sql_data({
        "video_id": video_id
    })
    bloom_df.to_csv(full_csv_path, index=False)

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
    for index, row in stats_df.iterrows():
        # check if page exists
        # if it does, skip
        # if it doesnt, create it
        row_date = row["date"].strftime("%Y-%m-%d")
        page_name = f'{video_id}-{row_date}.md'
        page_folder = f'{video_id}'
        page_path = f"{base_path}/{page_folder}/{curr_year_month}"
        # force overwrite pages where row["date"] == current date
        # as data is still trickling in
        if os.path.exists(f"{page_path}/{page_name}") and row["date"].strftime("%Y-%m-%d") != curr_date:
            print(f"{page_path}/{page_name} exists, skipping")
            continue
        # grab each chunk of data from bloomberg
        # get count
        iter_count = int(row['count'])
        end_point = curr_parsed + iter_count
        temp_df = bloom_df.iloc[curr_parsed:int(end_point)]
        curr_parsed += int(iter_count)
        # todo create markdown page from data, make reusing function
        # TODO update page_name 
        create_md_page(temp_df, {
            "video_id": video_id,
            "page_path": page_path,
            "created_at": row["date"],
            "path_name": page_name
        })
        page_paths.append(f"/blog/{page_name.replace('.md', '')}")
        # append .nojekyll file to each folder
        with open(f"{page_path}/.nojekyll", "w") as f:
            f.write("")
        print(f"{page_name} created")
    # create_markdown_page(chunk)
    # group csv by date in days
    # curr date in DD-MM-YYYY
    
    # TODO base this content on files in output folder
    with open(f"{base_path}/{video_id}/{video_id}.md", "w") as f:
        page_header = f"""---
pubDate: "{curr_date}"
category: "video_index"
title: "{video_id} - index file"
description: "Ornare cum cursus laoreet sagittis nunc fusce posuere per euismod dis vehicula a, semper fames lacus maecenas dictumst pulvinar neque enim non potenti. Torquent hac sociosqu eleifend potenti."
image: "~/assets/images/hero.jpg"
---\n"""
        f.write(page_header)
        f.write("\n")
        for page in page_paths:
            f.write(f"<a href='{page}'>{page}</a>\n")
    pass 

def main():
    # read config and validate env vars
    create_md_pages({})

if __name__ == "__main__":
    # for continuous livestreams, we have a post a page
    # grab all data from sql, and then split into chunks based on day
    # then output markdown pages for each day
    main()
