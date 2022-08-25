# script to grab data from pscale and output markdown pages that are compatible with astro posts
import os 
import mysql.connector
import pandas as pd
import spacy
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

def fetch_sql_data(video_id: str):
    # yes this is sql injection
    # personal project, not accessible to public where video_id is a string
    query = f"SELECT * FROM {video_id}"
    # Dont care about previous data
    new_df = pd.read_sql(query, con=connect_to_db())
    return new_df

def create_mdx_page(df: pd.DataFrame, cfg: dict):
    video_id = cfg.get("video_id", "dp8PhLsUcFE")
    page_path = f"{cfg.get('page_path')}"
    page_name = cfg.get("path_name", "index.mdx")
    page_entries_for = cfg.get("created_at", "2020-01-01")
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
                "spacy": rendered_spacy
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
    with open(mdx_path, "w") as f:
        page_header = f"""---
            pubDate: "{page_entries_for}"
            title: "AstroWind template in depth"
            description: "Ornare cum cursus laoreet sagittis nunc fusce posuere per euismod dis vehicula a, semper fames lacus maecenas dictumst pulvinar neque enim non potenti. Torquent hac sociosqu eleifend potenti."
            image: "~/assets/images/hero.jpg"
            ---
        \n
        """
        f.write(page_header) 
        for item in page_data:
            f.write(f"{item['spacy']}\n")
        pass
    return page_data

def create_mdx_pages(config: dict):
    cts_cfg = config.get("cts", {})
    # read video_id from livestream config
    # redo this entire logic later
    bloomberg_cfg = cts_cfg.get("bloomberg", {})
    bloomberg_id = bloomberg_cfg.get("video_id", "dp8PhLsUcFE")
    # if bloomberg.csv exists, skip grabbing it 
    if os.path.exists("bloomberg.csv"):
        print("Bloomberg.csv exists, skipping fetch")
    else:
        # read video_id from livestream config
        bloom_df = fetch_sql_data(bloomberg_id)
        bloom_df.to_csv("bloomberg.csv", index=False)

    bloom_df = pd.read_csv("bloomberg.csv", index_col=False)
    stats_df = (pd.to_datetime(bloom_df['created_at'])
       .dt.floor('d')
       .value_counts()
       .rename_axis('date')
       .reset_index(name='count'))
    # sort by date
    stats_df = stats_df.sort_values(by=['date'])
    # drop all rows with Unnamed in the column name
    curr_parsed = 0
    for index, row in stats_df.iterrows():
        # grab each chunk of data from bloomberg
        # get count
        iter_count = int(row['count'])
        end_point = curr_parsed + iter_count
        temp_df = bloom_df.iloc[curr_parsed:int(end_point)]
        curr_parsed += int(iter_count)
        # todo create markdown page from data, make reusing function
        page_name = f'{row["date"].strftime("%Y-%m-%d")}.md'
        page_folder = f'{bloomberg_id}'
        base_path = "src/data/posts"
        page_path = f"{base_path}/{page_folder}"
        create_mdx_page(temp_df, {
            "video_id": bloomberg_id,
            "page_path": page_path,
            "created_at": row["date"],
            "path_name": page_name
        })
        print(f"{page_name} created")
    # create_markdown_page(chunk)
    # group csv by date in days
    pass 

def main():
    # read config and validate env vars
    create_mdx_pages({})

if __name__ == "__main__":
    # for continuous livestreams, we have a post a page
    # grab all data from sql, and then split into chunks based on day
    # then output markdown pages for each day
    main()
    pass
