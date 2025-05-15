import requests
import pandas as pd
from datetime import datetime, timedelta
from src.PostgresConnector import PostgresConnector
import os

import dotenv
dotenv.load_dotenv()

# Define the API endpoint
API_URL = "https://www.hkab.org.hk/api/hibor"

# Define the date range
end_date = datetime.now()
period = os.getenv("PERIOD", 2)
start_date = end_date - timedelta(days=(period - 1))
load_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data_columns = [
    "overnight",
    "one_week",
    "two_weeks",
    "one_month",
    "two_months",
    "three_months",
    "four_months",
    "five_months",
    "six_months",
    "seven_months",
    "eight_months",
    "nine_months",
    "ten_months",
    "eleven_months",
    "twelve_months",
    "year",
    "month",
    "day",
    "date",
    "time",
    "is_holiday",
    "ds_is_fb",
    "content2_en",
    "content3_en",
    "content4_en",
    "content2_b5",
    "content3_b5",
    "content4_b5",
    "ds_fb_msg_1_en",
    "ds_fb_msg_1_b5",
    "ds_fb_msg_2_en",
    "ds_fb_msg_2_b5",
    "snapshot_date"
]

conn = PostgresConnector(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

if __name__ == "__main__":
    # Initialize an empty list to store the data
    data_list = []

    # Loop through each date in the range
    current_date = start_date
    while current_date <= end_date:
        # Format the date parameters
        year = current_date.year
        month = current_date.month
        day = current_date.day

        # Make the GET request
        response = requests.get(f"{API_URL}?year={year}&month={month}&day={day}")

        # Check the response status
        if response.status_code == 200:
            print(f"Request successful for {current_date.strftime('%Y-%m-%d')}!")
            data = response.json()

            # Add snapshot date
            data["snapshot_date"] = current_date.strftime("%Y-%m-%d")
            data_list.append(data)
        else:
            print(f"Request failed for {current_date.strftime('%Y-%m-%d')} with status code: {response.status_code}")

        # Move to the next day
        current_date += timedelta(days=1)

    # Convert the data list to a pandas DataFrame
    df = pd.DataFrame(data_list)
    df.columns = data_columns
    df["load_datetime"] = load_datetime

    # Display the DataFrame
    print(df.head())
    print(df.columns)

    # Save the DataFrame to PostgreSQL
    conn.write_data(df, "hkab_hibor", 
            if_exists="upsert", 
            conflict_columns=["snapshot_date"],
            index=False)
    print(f"Data saved to table 'hkab_hibor'")
