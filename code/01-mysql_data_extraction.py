# First of all, let's import the necessary libraries. As direct SQL-CSV download was not possible, we have to use a mySQL connector library (after some investigation on how it works)
import pandas as pd
import mysql.connector

# ------------------- DATA EXTRACTION FROM SQL DATABASE ------------------
# Let's start with the connection to the SQL database and export the contained data to a CSV file

# We have to create a variable "conn" to inidcate the DB access details, call the library and use the .connect() function
conn = mysql.connector.connect(
    host="51.94.152.149",
    user="test_user",
    password="q1w2e3r4",
    database="dataAnalistPaymentsV3"
)

# We have to create a query with SQL syntax to select all the desired data from the above database
query = "SELECT * FROM transactions"

# The function pd.read_sql() needs: 1)The SQL-sytanx "query" we just created, 2)The connection details like IP, user, pass, db (conn)
transactions = pd.read_sql(query, conn)

# Now that we are connected, we can export all the dataframe to a CSV file, indicating the name and no index column, as it already has one
transactions.to_csv("transactions_db.csv", index=False)

conn.close()
# ------------------- DATA EXTRACTED FROM SQL DATABASE ------------------
