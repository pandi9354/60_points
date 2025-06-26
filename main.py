import pandas as pd
import pymssql
import time
import logging
from kiteconnect import KiteConnect
from datetime import datetime
import os
import requests
from decimal import Decimal

# Kite API setup (replace with your credentials)
api_key = "ful79ld7x628erw5"
api_secret = "3mpk51e2vmg2tfkj15w9xp4gepdhn56x"

kite = KiteConnect(api_key=api_key)
request_token = input("Enter the request token: ").strip()

try:
    data = kite.generate_session(request_token, api_secret)
    access_token = data["access_token"]
    kite.set_access_token(access_token)
    logging.info("Access Token generated and set successfully!")
except Exception as e:
    logging.error(f"Error generating access token: {e}")
    exit()

# Database configuration
server = "server"
user = "sa"
password = "Password@123"
database = "trading"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def analyze_ltp():
    try:
        while True:  
            # Fetch the last 5 rows of LTP data from the database
            with pymssql.connect(server, user, password, database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(""" 
                        SELECT TOP 5 id, ltps, signal_time
                        FROM table_4 
                        WHERE CONVERT(DATE, signal_time) = CONVERT(DATE, GETDATE())
                        ORDER BY id DESC
                    """)
                    rows = cursor.fetchall()

            if len(rows) < 5:
                print("Not enough data to analyze for the current date.")
                time.sleep(12)  # Wait before retrying
                continue

            # Reverse the records to chronological order
            records = rows[::-1]
            ltps = [row[1] for row in records]
            print(f"LTPs: {ltps}")

            # Check for BUY/SELL signals (Removed India VIX Condition)
            if all(ltps[i] <= ltps[i + 1] for i in range(4)) and (ltps[4] - ltps[0] >= 50):
                signal = "BUY"
            elif all(ltps[i] >= ltps[i + 1] for i in range(4)) and (ltps[0] - ltps[4] >= 50):
                signal = "SELL"
            else:
                signal = None

            # Process the signal if generated
            if signal:
                print(f"Signal generated: {signal}, LTP: {ltps[-1]}")
                process_signals(signal, ltps[-1])  
                break  
            else:
                print("No signal generated. Continuing to analyze...")
                time.sleep(12)  # Sleep for 12 seconds before checking again

    except Exception as e:
        logging.error(f"Error analyzing LTP: {e}")

def process_signals(signal, ltp):
    try:
        if not signal or not ltp:
            print("No valid signal or LTP to process.")
            return

        # Calculate the strike price
        strike = round(ltp / 100) * 100
        option_type = "CE" if signal.upper() == "BUY" else "PE"
        print(f"Signal: {signal}, LTP: {ltp}, Strike: {strike}{option_type}")

        # Check if the Excel file exists
        file_path = "output.xlsx"
        if not os.path.exists(file_path):
            print(f"Excel file not found: {file_path}")
            return

        # Read the Excel file
        df = pd.read_excel(file_path)
        filtered_df = df[
            (df["strike"] == strike) & 
            (df["instrument_type"] == option_type)
        ]

        # Check for matching instrument
        if not filtered_df.empty:
            tradingsymbol = filtered_df.iloc[0]["tradingsymbol"]
            print(f"Tradingsymbol found: {tradingsymbol}")
            instrument_symbol = f"BFO:{tradingsymbol}"
            ltp_data = kite.ltp([instrument_symbol])

            if instrument_symbol in ltp_data:
                fetched_ltp = ltp_data[instrument_symbol]["last_price"]
                print(f"LTP for {instrument_symbol}: {fetched_ltp}")

                # Insert into exit_enrty table
                with pymssql.connect(server, user, password, database) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT MAX(EntryID) FROM exit_entry")
                        max_entry_id = cursor.fetchone()[0] or 0  # Handle NULL case
                        max_entry_id += 1

                        insert_query = """
                            INSERT INTO exit_entry (EntryID, entry_premium , trading_symbol)
                            VALUES (%s, %s, %s)
                        """
                        cursor.execute(insert_query, (max_entry_id, fetched_ltp, tradingsymbol))
                        conn.commit()
                        print(f"Inserted EntryID {max_entry_id} with premium {fetched_ltp}.")
                        
                        exit_order(tradingsymbol, signal,ltp )
            else:
                print(f"LTP data not found for instrument: {instrument_symbol}")
        else:
            print("No matching instrument found for placing order.")
    except Exception as e:
        logging.error(f"Error processing signals: {e}")

def fetched_strike_price(tradingsymbol):
    """
    Fetch the LTP (Last Traded Price) for the given tradingsymbol.
    """
    try:
        print(f"Tradingsymbol found: {tradingsymbol}")
        instrument_symbol = f"BFO:{tradingsymbol}"
        ltp_data = kite.ltp([instrument_symbol])

        if instrument_symbol in ltp_data:
            fetched_ltp = ltp_data[instrument_symbol]["last_price"]
            print(f"LTP for {instrument_symbol}: {fetched_ltp}")
            return fetched_ltp
    except Exception as e:
        logging.error(f"Error fetching LTP for {tradingsymbol}: {e}")
        return None


def fetch_latest_ltp():
    """
    Fetch the latest LTP from the database.
    """
    try:
        with pymssql.connect(server, user, password, database) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT TOP (1) [ltps]
                    FROM [dbo].[table_4]
                    ORDER BY [signal_time] DESC;
                """)
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        logging.error(f"Error fetching latest LTP: {e}")
        return None
    
def update_time_exit(fetched_ltp, tradingsymbol, column):
    """
    Update the time_exit and exit_premium in the database and send details to WhatsApp, including total PnL.
    """
    try:
        with pymssql.connect(server, user, password, database) as conn:
            with conn.cursor() as cursor:
                # Fetch the EntryID for the given tradingsymbol
                cursor.execute("""
                    SELECT MAX(EntryID) FROM exit_entry
                """)
                result = cursor.fetchone()
                if not result or result[0] is None:
                    logging.error(f"No matching entry found for tradingsymbol {tradingsymbol}.")
                    return
                entry_id = result[0]

                # Construct the query dynamically for the column
                query = f"""
                    UPDATE exit_entry
                    SET {column} = %s
                    WHERE EntryID = %s
                """
                cursor.execute(query, (fetched_ltp, entry_id))
                conn.commit()
 
        logging.info(f"Exit details successfully updated for {tradingsymbol}.")

    except Exception as e:
        logging.error(f"Error updating {column} for {tradingsymbol}: {e}")


from decimal import Decimal

def exit_order(tradingsymbol, signal, initial_ltp, max_retries=500, retry_interval=12):
    try:
        ordered_ltp=Decimal(initial_ltp)
        high_or_low = Decimal(initial_ltp)  # Convert to Decimal for consistency
        exit_condition_met = False
        retries = 0

        def calculate_quantity(delta, mainQty):
            """Determine quantity dynamically based on the LTP delta."""
            if delta >= 40:
                quantity = int(100 * (mainQty / 100))
                return mainQty - quantity, quantity
            elif delta >= 30:
                return mainQty - 25, 25
            elif delta >= 20:
                return mainQty - 25, 25
            elif (signal == "BUY" and high_or_low - current_ltp >= 40):
                quantity = int(100 * (mainQty / 100))
                return mainQty - quantity, quantity
            elif (signal == "SELL" and current_ltp - high_or_low >= 40):
                quantity = int(100 * (mainQty / 100))
                return mainQty - quantity, quantity
            return mainQty, 0  # Default case: no change

        mainQty = 100
        colunm = "first_exit"

        while not exit_condition_met and retries < max_retries:
            # Fetch the current LTP from the database
            current_ltp = fetch_latest_ltp()
            print(current_ltp)
            if current_ltp is None:
                logging.info("Current LTP not found. Retrying...")
                time.sleep(retry_interval)
                continue

            current_ltp = Decimal(current_ltp)  # Ensure current_ltp is Decimal
            logging.info(f"High or Low: {high_or_low}, Current LTP: {current_ltp}")

            if signal == "BUY" :
                delta = current_ltp - ordered_ltp
                print(delta)
                mainQty, quantity = calculate_quantity(delta, mainQty)
                if quantity > 0:
                    fetched_ltp = fetched_strike_price(tradingsymbol)
                    update_time_exit(fetched_ltp, tradingsymbol, colunm)
                    if colunm == "first_exit":
                        colunm = "second_exit"
                    elif colunm == "second_exit":
                        colunm = "third_exit"
                    elif colunm == "third_exit":
                        colunm = "fourth_exit"
                    if mainQty == 0:
                        exit_condition_met = True
                else:
                    high_or_low = max(high_or_low, current_ltp)
                    logging.info(f"Updated high_or_low to {high_or_low} for BUY signal.")

            elif signal == "SELL" :
                delta = ordered_ltp - current_ltp
                print(delta)
                mainQty, quantity = calculate_quantity(delta, mainQty)
                if quantity > 0:
                    fetched_ltp = fetched_strike_price(tradingsymbol)
                    update_time_exit(fetched_ltp, tradingsymbol, colunm)
                    if colunm == "first_exit":
                        colunm = "second_exit"
                    elif colunm == "second_exit":
                        colunm = "third_exit"
                    elif colunm == "third_exit":
                        colunm = "fourth_exit"
                    if mainQty == 0:
                        exit_condition_met = True
                else:
                    high_or_low = min(high_or_low, current_ltp)
                    logging.info(f"Updated high_or_low to {high_or_low} for SELL signal.")

            retries += 1
            time.sleep(retry_interval)

        if retries >= max_retries:
            logging.warning(f"Max retries reached for {tradingsymbol}. Exiting loop.")
        else:
            logging.info(f"Exit condition met for {tradingsymbol}.")
        analyze_ltp()
    except Exception as e:
        logging.error(f"Error during exit order monitoring: {e}")

   

if __name__ == "__main__":
    analyze_ltp()
    # ghfxdfuigg
    # hgfgfh