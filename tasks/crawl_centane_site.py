import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from src.PostgresConnector import PostgresConnector
import os
import dotenv
dotenv.load_dotenv()

query_id = "nha2hey2xopcy"
conn = PostgresConnector(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)


# Fetch data from centanet
url = f"https://hk.centanet.com/findproperty/list/transaction?q={query_id}"
res = requests.get(url, verify=False)
html = res.content.decode()

# Parse the html
soup = BeautifulSoup(html, 'html.parser')

# Get all rows in tbody
rows = soup.find_all("div", attrs={"class": "bx--structured-list-row"})
data = [[item.find("div").text for item in row.find_all("div", attrs={"class": "bx--structured-list-td"}) if item.find("div")] for row in rows]

# Get rows in thead
header_row = soup.find("div", attrs={"class": "bx--structured-list-row--header-row"})
header = [item.text for item in header_row.find_all("div", attrs={"class": "bx--structured-list-th"})]

eng_header = [
    "date",
    "address",
    "interval",
    "floor_plan",
    "transaction_price",
    "change",
    "area_net",
    "price_per_sq_ft_net",
    "source"
]

df = pd.DataFrame(data, columns=eng_header)

# Basic cleansing
df = df.dropna()
for col in df.columns:
    df[col] = df[col].apply(lambda x: x.replace("\n", '').strip())

print(df)

df["query_id"] = query_id
df["as_of_datetime"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

# Load to postgres database
conn.write_data(df, "centanet_property_transaction", 
                if_exists="overwrite", 
                conflict_columns=["query_id", "Date", "Address"], 
                index=False)

