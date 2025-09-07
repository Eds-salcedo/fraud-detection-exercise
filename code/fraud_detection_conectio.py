"""Fraud Detection Data Analysis"""

# First of all, let's import the necessary libraries.
import pandas as pd
import mysql.connector

# LIST OF CONSTANTS
INPUT_CSV = './transactions_db.csv'
OUTPUT_HIGH_TRANSACTION_DAYS_CSV = "1_high_transaction_days.csv"
OUTPUT_HIGH_INCOME_DAYS_CSV = "2_high_income_days.csv"
OUTPUT_HIGH_VALUE_TRANSACTIONS_CSV = "3_high_value_transactions.csv"
OUTPUT_DAILY_INCOME_BY_SITE_CSV = "4_daily_income_by_site.csv"
OUTPUT_DAILY_TRANSACTIONS_BY_SITE_CSV = "5_daily_trans_by_site.csv"
OUTPUT_FAST_PURCHASES_BY_CUSTOMER = "6_suspicious_fast_purchases.csv"
OUTPUT_SUSPICIOUS_SITES_ACTIVITY_CSV = "7_suspicious_sites_activity.csv"
# END CONSTANTS


# Now that we created the CSV, we may proceed to read the dataset, ceating a dataframe
df = pd.read_csv(INPUT_CSV)


# ---------- DEFINING FUNCTIONS TO BE USED LATER ----------
def explore_data():
    # We can check the headers and first rows of the dataframe to see if the data was imported correctly
    print(df.info())
    print(df.head())


def initialize_data():
    # We need to check the date format first
    df["trans_datetime"] = pd.to_datetime(df["trans_datetime"])
    # In case that the amount column is not in number format, we need to force/coerce its convertion
    df["trans_amount"] = pd.to_numeric(df["trans_amount"], errors="coerce")

    # Then we need to create a new column with dates without time, to use it as reference for day grouping
    df["trans_date"] = df["trans_datetime"].dt.date


def get_summary_statistics(column):
    return {
        "Avg": column.mean(),
        "Min": column.min(),
        "Max": column.max(),
        "Std": column.std()
    }


def save_high_transaction_days_to_csv(daily_transactions):
    # Previous filter of days with transactions above 9,700
    high_transaction_days = daily_transactions[daily_transactions["transaction_count"] > 9700]
    high_transaction_days.to_csv(OUTPUT_HIGH_TRANSACTION_DAYS_CSV, index=False)


def save_high_income_days_to_csv(df, daily_income):
    # Previous filter of days with income above $565,000
    high_income_days = daily_income[daily_income["sum_daily_income"] > 565000]
    high_income_transactions = df[df["trans_date"].isin(
        high_income_days["trans_date"])]

    high_income_transactions.to_csv(OUTPUT_HIGH_INCOME_DAYS_CSV, index=False)


def save_high_value_transactions_csv(customer_spending):
    # Previous filter of transactions above $200 and more than 3 transactions per day
    high_value_transactions = customer_spending[
        (customer_spending["daily_spending"] > 200) &
        (customer_spending["daily_transactions"] > 3)
    ]
    high_value_transactions.reset_index(drop=True).to_csv(
        OUTPUT_HIGH_VALUE_TRANSACTIONS_CSV, index=False)


def save_daily_trans_by_site_csv(daily_trans_site):
    # Previous filter of days when any site had more than 15 transactions
    high_trans_site = daily_trans_site[daily_trans_site["site_count"] > 15]
    high_trans_site.to_csv(
        OUTPUT_DAILY_TRANSACTIONS_BY_SITE_CSV, index=False)


def save_daily_income_by_site_csv(daily_site_income):
    # Previous filter of days when any site had more than $1,000 income
    daily_site_income_filtered = daily_site_income[daily_site_income["daily_income"] > 1000]
    daily_site_income_filtered.to_csv(
        OUTPUT_DAILY_INCOME_BY_SITE_CSV, index=False)
    return daily_site_income_filtered


def save_fast_purchases_to_csv(fast_purchases):
    fast_purchases.to_csv(OUTPUT_FAST_PURCHASES_BY_CUSTOMER, index=False)


def save_suspicious_sites_activity_csv(sites_visited_per_day):
    # Let's filter customers who purchased from 2 or more sites in the same day
    suspicious_sites = sites_visited_per_day[sites_visited_per_day["unique_sites"] > 3]
    suspicious_sites.to_csv(OUTPUT_SUSPICIOUS_SITES_ACTIVITY_CSV, index=False)

# ---------- END OF FUNCTION DEFINITIONS ----------


def main():
    initialize_data()

    # 1) LET'S OBTAIN THE AVERAGE NUMBER OF DAILY TRANSACTIONS AND CHECK FOR OUTLIERS

    # Now we can group all the dates and count the number of transactions received during each day group
    daily_transactions = df.groupby(
        "trans_date")["id_transaction"].count().reset_index()
    # We should rename the new column so it's clear it shows a counting of transactions IDs, not the original column
    daily_transactions.rename(
        columns={"id_transaction": "transaction_count"}, inplace=True)

    print("----------------- Daily transactions:")
    print(daily_transactions)

    # Now that we have a full list of grouped daily transactions, we can calculate the average & outliers
    daily_transactions_summary = get_summary_statistics(
        daily_transactions["transaction_count"])
    print("----------------- Daily transactions summary:")
    print(daily_transactions_summary)
    # Results: {'Avg':9,585, 'Min':9,285, 'Max':9,789, 'Std':95} // I see no big outlier, around 2% variation max when the standard deviation is 1% of the avg

    # ** Still, considering that the std is 1% of the avg, let's flag the days when transactions were above 9,700 to a CSV
    high_days = daily_transactions[daily_transactions["transaction_count"] > 9700]
    print(high_days)
    save_high_transaction_days_to_csv(daily_transactions)

    # 2) LET'S CALCULATE THE AVERAGE INCOME BY DAY, THEN BY CUSTOMER AND BY SITE, WITH VARIATIONS

    # A) AVERAGE AND VARIATION OF DAILY INCOME & OUTLIERS
    daily_income = df.groupby("trans_date", as_index=False)[
        "trans_amount"].sum()
    daily_income.rename(
        columns={"trans_amount": "sum_daily_income"}, inplace=True)
    print("----------------- Daily income:")
    print(daily_income)

    # Now that we have a full list of grouped daily transactions, we can calculate the average & outliers
    daily_income_summary = get_summary_statistics(
        daily_income["sum_daily_income"])
    print("----------------- Daily income summary:")
    print(daily_income_summary)
    # Results: {'Avg':$552,164; 'Min':$506,255; 'Max':$575,948, 'Std':$11,026} // I see no big outliers, around 2% variation

    # ** Still, considering that the std is 2% of the avg, let's flag the days when income was above $565,000 to a CSV
    save_high_income_days_to_csv(df, daily_income)

    # B) AVERAGE CUSTOMER SPENDING & OUTLIERS
    customer_spending = df.groupby(["trans_date", "customer_id"]).agg(
        daily_spending=("trans_amount", "sum"),
        daily_transactions=("id_transaction", "count")
    ).reset_index()
    print("----------------- Customer spending & transactions per day:")
    print(customer_spending)

    # Here, the new variable with the summary matrics needs to be a little different as we need to calculate the metrics for each separate column of customer_spending (Spending sum & Transactions count)
    customer_spending_summary = {
        "spending_metrics": get_summary_statistics(customer_spending["daily_spending"]),
        "transactions_metrics": get_summary_statistics(customer_spending["daily_transactions"])
    }
    print("----------------- Customer spending summary:")
    print(customer_spending_summary)
    # Results: {'Avg':$148; 'Min':$0; 'Max':$13k; 'Std':$145} // I see big outliers, a peak of $13K, while avg is $148 & usual variation 98%
    # Results: {'Avg':2.5 ; 'Min':1 ; 'Max':14; 'Std':1.6} // I see big outliers, a peak of 14, while avg is 2 to 3 & usual variation 40%

    # Let's flag the transactions worth more than $250 up to the max
    # There are 11k transactions above $200, which is 0.5% of total transactions (+1.9M), let's save them in a CSV for an Excel analysis
    save_high_value_transactions_csv(customer_spending)

    # C) AVERAGE METRICS BY SITE

    # SITE TRANSACTION COUNT
    daily_trans_site = df.groupby(["trans_date", "site_id"]).agg(
        site_count=("site_id", "count")
    ).reset_index()

    print("----------------- Daily transactions by site:")
    print(daily_trans_site)

    trans_site_summary = get_summary_statistics(daily_trans_site["site_count"])
    print("----------------- Daily transactions by site summary:")
    print(trans_site_summary)

    # Results: {'Avg':9; 'Min':1; 'Max':34; 'Std':3.6} // I see big outliers, a peak around 34, while avg is 9 & usual variation 3 to 4.

    # Let's flag the days when any site had more than 15 transactions & download the results to a CSV for an Excel analysis
    save_daily_trans_by_site_csv(daily_trans_site)

    # SITE INCOME AMOUNT
    daily_site_income = df.groupby(["trans_date", "site_id"], as_index=False)[
        "trans_amount"].sum()
    daily_site_income.rename(
        columns={"trans_amount": "daily_income"}, inplace=True)
    print(daily_site_income)

    site_income_summary = get_summary_statistics(
        daily_site_income["daily_income"])
    print("----------------- Daily income by site summary:")
    print(site_income_summary)
    # Results: {'Avg':$522; 'Min':$0; 'Max':$11k; 'Std':$275} // I see big outliers, a peaks around $3,176, while avg is $522 & usual variation 60%

    # Let's flag the days when any site had more than $1,000 income & download the results to a CSV for an Excel analysis
    save_daily_income_by_site_csv(daily_site_income)

    # 3) AFTER ANALYZING THE DATASET, LET'S CHECK THE TIME DISTRIBUTION OF TRANSACTIONS TO FLAG UNUSUAL ACTIVITY

    # First, we need to create a new column with only the time difference of the current transaction vs its previous one, grouped by customer
    # After exploring the logic behind the trans_time column, I realized it was not totally useful for this task, so let's create a new one
    sorted_purch_times_by_customer = df.sort_values(
        by=["customer_id", "trans_date", "trans_datetime"])
    sorted_purch_times_by_customer["time_diff"] = sorted_purch_times_by_customer.groupby(
        ["customer_id", "trans_date"])["trans_datetime"].diff()

    purchases_by_customer_summary = get_summary_statistics(
        sorted_purch_times_by_customer["time_diff"].dt.total_seconds())

    print("----------------- Sorted purchase times by customer with differences:")
    print(purchases_by_customer_summary)
    # Results: {'Avg': 13.4k sec; 'Min': 0 sec; 'Std': 11.7k sec} // I see that some transactions are made in the same second, but the avg is 13k sec between transactions

    # Let's flag the transactions made in less than 300 seconds from the previous one
    fast_purchases = sorted_purch_times_by_customer[sorted_purch_times_by_customer["time_diff"] <= pd.Timedelta(
        seconds=300)]
    print("----------------- Fast purchases (< 300 seconds):")
    print(fast_purchases)

    save_fast_purchases_to_csv(fast_purchases)

    # 4) CHECKING HOW MANY TIMES A CUSTOMER PURCHASES IN ALL SITES, IN THE SAME DAY

    # Contar la cantidad de transacciones por cliente, por día y por sitio
    # Let's count the number of transactions per customer, per day and per site. nunique() is like a =COUNTUNIQUE() in Excel.
    sites_visited_per_day = (df.groupby(["customer_id", "trans_date"])[
        "site_id"].nunique().reset_index(name="unique_sites"))

    sites_per_day_summary = get_summary_statistics(
        sites_visited_per_day["unique_sites"])
    print("----------------- Unique sites per customer per day:")
    print(sites_per_day_summary)

    # Results: {'Avg':2.5; 'Min':1; 'Max':13***; 'Std':1.5} // I see a peak of 13 different sites where a customer made purchases in the same day, while avg is 2.5 & usual variation +/-1.5

    save_suspicious_sites_activity_csv(sites_visited_per_day)


if __name__ == "__main__":
    main()
