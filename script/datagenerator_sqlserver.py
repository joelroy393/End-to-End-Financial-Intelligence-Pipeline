import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sqlalchemy
import urllib

# Set seed for reproducibility
random.seed(42)

def generate_data(num_rows=2500):
    """
    Iteration 3: SQL Server (Enterprise) Engineering.
    Pushes both Actuals and Budget data directly to a local SQL Server instance.
    Ensures Date columns are stored as native DateTime types for Power BI.
    """
    # --- SQL SERVER CONFIGURATION ---
    # Change 'LOCALHOST\SQLEXPRESS' to your actual Server Name from SSMS
    SERVER_NAME = r'LOCALHOST\SQLEXPRESS' 
    DATABASE_NAME = 'FinancePortfolioDB'
    
    # 1. Setup Data Categories
    cities = {
        'Windsor': ('Ontario', 'Canada', 42.3149, -83.0364),
        'Toronto': ('Ontario', 'Canada', 43.6532, -79.3832),
        'Detroit': ('Michigan', 'United States', 42.3314, -83.0458),
        'Kitchener': ('Ontario', 'Canada', 43.4516, -80.4925),
        'Ottawa': ('Ontario', 'Canada', 45.4215, -75.6972),
    }
    payment_modes = ['Credit Card', 'Debit Card', 'E-Transfer', 'Cash']
    categories = {
        'Expense': ['Rent', 'Groceries', 'Utilities', 'Dining', 'Tech', 'Transport', 'Cybersecurity', 'Entertainment', 'Healthcare'],
        'Income': ['Salary', 'Freelance', 'Investments', 'Gift']
    }

    # 2. Generate Fact_Transactions Dataframe
    start_date = datetime(2025, 1, 1)
    actuals_list = []
    for _ in range(num_rows):
        type_choice = random.choices(['Expense', 'Income'], weights=[0.82, 0.18])[0]
        category = random.choice(categories[type_choice])
        date = start_date + timedelta(days=random.randint(0, 450))
        amt = 1200.00 if category == 'Rent' else round(random.uniform(10, 400), 2)
        city = random.choice(list(cities.keys()))
        province, country, lat, lon = cities[city]
        
        actuals_list.append([
            date, category, type_choice, amt,
            city, province, country, lat, lon, random.choice(payment_modes),
            f"TXN-{random.randint(100000, 999999)}"
        ])

    df_actuals = pd.DataFrame(actuals_list, columns=['Date', 'Category', 'Type', 'Amount', 'City', 'Province', 'Country', 'Latitude', 'Longitude', 'PaymentMode', 'TransactionID'])
    
    # CRITICAL: Ensure the 'Date' column is a proper datetime object
    df_actuals['Date'] = pd.to_datetime(df_actuals['Date'])

    # 3. Generate Fact_Budget Dataframe
    budget_list = []
    monthly_targets = {
        'Groceries': 500, 'Dining': 300, 'Utilities': 200, 
        'Tech': 150, 'Transport': 250, 'Cybersecurity': 100, 
        'Entertainment': 100, 'Healthcare': 80, 'Rent': 1200
    }

    for year in [2025, 2026]:
        for month in range(1, 13):
            for cat, target in monthly_targets.items():
                budget_list.append([datetime(year, month, 1), cat, target])

    df_budget = pd.DataFrame(budget_list, columns=['BudgetMonth', 'Category', 'BudgetAmount'])
    
    # CRITICAL: Ensure the 'BudgetMonth' column is a proper datetime object
    df_budget['BudgetMonth'] = pd.to_datetime(df_budget['BudgetMonth'])

    # 4. Connect and Push to SQL Server
    print(f"Connecting to SQL Server: {SERVER_NAME}...")
    
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )
    
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    try:
        # Push Fact_Transactions
        df_actuals.to_sql('Fact_Transactions', engine, if_exists='replace', index=False, dtype={
            'Date': sqlalchemy.DateTime()
        })
        # Push Fact_Budget
        df_budget.to_sql('Fact_Budget', engine, if_exists='replace', index=False, dtype={
            'BudgetMonth': sqlalchemy.DateTime()
        })
        
        print("---")
        print("SUCCESS: Data uploaded to SQL Server")
        print(f"Table 1: [Fact_Transactions] ({len(df_actuals)} rows)")
        print(f"Table 2: [Fact_Budget] ({len(df_budget)} rows)")
        print("---")
    except Exception as e:
        print(f"Connection Error: {e}")
        print("\nTIP 1: Ensure 'ODBC Driver 17 for SQL Server' is installed.")
        print("TIP 2: Make sure 'FinancePortfolioDB' exists in SSMS.")

if __name__ == "__main__":
    generate_data(2500)