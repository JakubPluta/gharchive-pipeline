import pprint

import duckdb
import logging


logging.basicConfig(level=logging.DEBUG)

S3_BUCKET = "datalake"
S3_KEY = "gharchive/raw"

aws_access_key_id = "USERNAME"
aws_secret_access_key = "PASSWORD"
endpoint_url = "localhost:9000"
source_path = "s3://datalake/gharchive/raw/2024-12-26/08/events.json.gz"
con = duckdb.connect()
con.install_extension("httpfs")
con.load_extension("httpfs")
con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
con.execute(f"SET s3_endpoint='{endpoint_url}'")
con.execute("SET s3_use_ssl=false")
con.execute("SET s3_url_style = 'path'")
con.execute(f"CREATE OR REPLACE TABLE raw AS FROM read_json_auto('{source_path}', ignore_errors=true)")

query = """
SELECT
id as event_id,
type as event_type,
created_at as event_created_at,
public as is_public,
actor.id as user_id,
actor.login as user_login,
actor.display_login as user_display_login,
repo.id as repo_id,
repo.name as repo_name,
repo.url as repo_url
FROM raw
"""
con.execute(f"CREATE OR REPLACE TABLE clean AS FROM ({query})")

table = con.table("clean")

pprint.pprint(table.show())


# print(data.fetch_df())

# data = duckdb.read_json(source_path)
# print(data)
