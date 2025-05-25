import requests
import json, os
import pandas as pd
from src.PostgresConnector import PostgresConnector

import dotenv
dotenv.load_dotenv()
from datetime import datetime

# Define the URL and payload
url = "https://hk.centanet.com/findproperty/api/Transaction/Search"
payload = {
    "amountRange": {},
    "bigPhotoMode": False,
    "day": "Day1095",
    "offset": 0,
    "order": "Descending",
    "pageSource": "search",
    "postType": "Sale",
    "size": 24,
    "sort": "InsOrRegDate"
}

# Add to the params
search_keywords = [
    "", "將軍澳廣場"
]

conn = PostgresConnector(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)
load_datetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
random_wait = int(os.getenv("RANDOM_WAIT", 0)) # seconds

def json_dumps(x):
    import json
    return json.dumps(x, ensure_ascii=False)

if __name__ == "__main__":

    search_keyword = search_keywords[1]
    for search_keyword in search_keywords:
        if search_keyword:
            payload["keyword"] = search_keyword

        # Make the POST request
        response = requests.post(url, json=payload)

        # Check the response status and print the result
        if response.status_code == 200:
            print("Request successful!")
            data = response.json()

            # Convert the data to a pandas DataFrame
            data_list = data.get("data", [])
            df = pd.DataFrame(data_list)

            # convert all dict and list columns to string
            for col in df.columns:
                if isinstance(df[col].iloc[0], (dict, list)):
                    df[col] = df[col].apply(json_dumps)

            df = df.rename({"id": "transactionId"}, axis = 1)
            df["keyword"] = search_keyword
            df["load_datetime"] = load_datetime

            # add regdate as null if missing
            if "regDate" not in df.columns:
                df["regDate"] = None

            # Save the DataFrame to PostgreSQL
            check = conn.write_data(df, "centaline_transaction", 
                    if_exists="upsert", 
                    conflict_columns=["transactionId"],
                    index=False)
            if not check:
                raise ValueError("Failed to write data to PostgreSQL")
            print(f"Data saved to table 'centaline_transaction'")
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
