import pandas as pd
import numpy as np
from datetime import datetime as dt
from decimal import Decimal as dec
from src.db.core import *
import uuid
import csv

# EXTRACT =================================================


def extract(file_path):

    # extract data from csv file and add column names
    data = pd.read_csv(
        file_path,
        names=[
            "datetime",
            "location_name",
            "customer_info",
            "basket",
            "payment_method",
            "total_cost",
            "card_details",
        ],
    )

    # convert data to dataframe
    df = pd.DataFrame(data)

    # remove rows with null values
    df.dropna(inplace=True)

    # convert values in column "datetime" to datetime objects
    df["datetime"] = pd.to_datetime(df["datetime"])

    # format float values in "total_value" column to 2 decimal places
    df["total_cost"] = df["total_cost"].round(decimals=2)

    return df

df = extract("data/2021-02-23-isle-of-wight.csv")

# TRANSFORM ===============================================

# Create dataframe with 1 row for each item purchased and cleanse data
def transform(df):
    breakdown = df.copy()
    reordered = breakdown[["datetime", "location_name", "payment_method", "total_cost", "basket"]] # Reorder columns
    normalised_data = [] # Empty list to append new rows of data with breakdown of items in basket
    for i, row in reordered.iterrows():
        new_row = {}
        for column in row.items():
            if column[0] != "basket":  # Iterates through all columns other than basket, and adds column name and respectice value to dictionary
                new_row[column[0]] = column[1]
            elif column[0] == "basket":
                basket_split = column[1].split(",")  # Splits basket row into list of values
                index = 0
                while index < len(basket_split):  # Stops when there are no more list_items to iterate through
                    new_row["size"] = basket_split[index]
                    new_row["name"] = basket_split[index + 1]
                    new_row["price"] = float(basket_split[index + 2])
                    split_type = new_row["name"].split("-")
                    try:
                        new_row["name"] = split_type[0].strip()
                        new_row["type"] = split_type[1].strip()
                    except IndexError:
                        new_row["name"] = split_type[0]
                        new_row["type"] = ""
                    normalised_data.append(
                        new_row.copy()
                    )  # Appends new row for each basket item - if there are multiple items purchased in one transaction, the same information for the non-basket key:value pairs is used, so only the product related columns will differ
                    index += 3
    return normalised_data

data = transform(df)

# LOAD ====================================================

def load(data):
    
    for row in data:
        
        # load location
        
        location_name = row["location_name"]
        
        # check if location already exists in the database
        get_locations = "SELECT * FROM location WHERE name = %s"
        result = check(conn, get_locations, [location_name])
        
        if result == []:
            location_id = str(uuid.uuid4())
            sql_insert_locations = "INSERT INTO location (location_id, name) VALUES (%s, %s)"
            update(conn, sql_insert_locations, (location_id, location_name))
        else:
            location_id = result[0][0]
        
        # load product
        
        product_size = row["size"]
        product_name = row["name"]
        product_type = row["type"]
        
        # check if product already exists in the database
        get_products = "SELECT * FROM product WHERE name = %s AND type = %s AND size = %s"
        result = check(conn, get_products, [product_name, product_type, product_size])
        
        if result == []:
            product_id = str(uuid.uuid4())
            sql_insert_products = "INSERT INTO product (product_id, name, type, size) VALUES (%s, %s, %s, %s)"
            update(conn, sql_insert_products, (product_id, product_name, product_type, product_size))
        else:
            product_id = result[0][0]
            
        # load transaction
        
        date_time = row["datetime"]
        payment_type = row["payment_method"]
        total_cost = row["total_cost"]
        
        get_transaction = "SELECT * FROM transaction WHERE date_time = %s AND total_cost = %s"
        result = check(conn, get_transaction, [date_time, total_cost])
        
        if result == []:
            transaction_id = str(uuid.uuid4())
            sql_insert_transaction = "INSERT INTO transaction (transaction_id, location_id, date_time, payment_type, total_cost) VALUES (%s, %s, %s, %s, %s)"
            update(conn, sql_insert_transaction, (transaction_id, location_id, date_time, payment_type, total_cost))
        else:
            transaction_id = result[0][0]
            
        # load basket
        
        price = row["price"]
        
        basket_id = str(uuid.uuid4())
        sql_insert_basket = "INSERT INTO basket(basket_id, transaction_id, product_id, price) VALUES (%s, %s, %s, %s)"
        update(conn, sql_insert_basket, (basket_id, transaction_id, product_id, price))

load(data)