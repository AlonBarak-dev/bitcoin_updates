import requests
import json
from datetime import datetime
import datetime
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3

DATABASE_LOCATION = "sqlite:///bitcoin_updates.sqlite"

# THE URL ADDRESS
URL = "https://api.coindesk.com/v1/bpi/currentprice.json"

request = requests.get(URL)
data = request.json()  # CREATING A JSON FILE

"""
    this method checks for garbage data
"""

def validation(df: pd.DataFrame) -> bool:
    # check for empty data
    if df.empty:
        raise Exception("No new data was generated")

    # check for duplicates, Not necessary because only one raw is generated at a time
    if pd.Series(["date"]).is_unique:
        pass
    else:
        raise Exception("Primary key is violated, found duplicates!")
    # check for nulls
    if df.isnull().values.any():
        raise Exception("Received NULL as data")

    return True


# END FOR VALIDATION METHOD
if __name__ == "__main__":

    usd_rate = []
    eur_rate = []
    gbp_rate = []
    # EXTRACTING THE DATA
    usd_rate.append(data["bpi"]["USD"]["rate_float"])
    gbp_rate.append(data["bpi"]["GBP"]["rate_float"])
    eur_rate.append(data["bpi"]["EUR"]["rate_float"])

    # generating today's date
    today_date = datetime.datetime.now()

    # DATA DICTIONARY
    rate_dictionary = {
        "DATE": today_date,
        "USD_RATE": usd_rate,
        "GBP_RATE": gbp_rate,
        "EUR_RATE": eur_rate,
    }
    # creating a data frame for the data using panda library
    rate_df = pd.DataFrame(rate_dictionary, columns=["DATE", "USD_RATE", "GBP_RATE", "EUR_RATE"])

    # validation test
    if validation(rate_df):
        print("The data is valid and ready for load")

    # connecting to the data base and creating a cursor
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('bitcoin_update.sqlite')
    cursor = conn.cursor()

    # creating a table for the data or add data in case the table already exists
    sql_query = """
        CREATE TABLE IF NOT EXISTS bitcoin_updates(
            date_time VARCHAR(200),
            USD_rate VARCHAR(200),
            GBP_rate VARCHAR(200),
            EUR_rate VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (date_time)
        )  
        """

    cursor.execute(sql_query)
    print("Opened database successfully")

    # load data to our database
    try:
        rate_df.to_sql("bitcoin_updates", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database!")

    conn.close()
    print("Closed database successfully")


    print(rate_df)
