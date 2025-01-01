import logging
import os
import time
from glob import glob
from typing import List, Tuple
import pandas as pd
import psycopg2
import requests
from homeharvest import scrape_property

format = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=format)


def condense_files(input_path: str = None, filter_str: str = None) -> str:
    logging.info(f"Condensing {filter_str} files in {input_path}")

    # Find all CSV files in the directory
    filter_path = input_path.replace('~',os.path.expanduser('~')) + f"*{filter_str}.csv"
    csv_files = glob(filter_path)

    logging.info(f"Filter path: {filter_path}")
    logging.info(f"CSV Files: {csv_files}")

    dtype = {
        "property_url": "str",
        "mls": "str",
        "mls_id": "Int64",
        "status": "str",
        "style": "str",
        "street": "str",
        "unit": "str",
        "city": "str",
        "state": "str",
        "zip_code": "str",
        "beds": "Int64",
        "full_baths": "Int64",
        "half_baths": "Int64",
        "sqft": "Int64",
        "year_built": "Int64",
        "days_on_mls": "Int64",
        "list_price": "Int64",
        "list_date": "str",
        "sold_price": "str",
        "last_sold_date": "str",
        "lot_sqft": "Int64",
        "price_per_sqft": "Int64",
        "latitude": "Float64",
        "longitude": "Float64",
        "stories": "Int64",
        "hoa_fee": "Int64",
        "parking_garage": "Int64",
        "primary_photo": "str",
        "alt_photos": "str",
    }

    output_file = ""
    try:
        dirname = os.path.dirname(csv_files[0])
        basename = os.path.basename(csv_files[0])
        new_basename = "_".join(basename.split("_")[0:1]) + f"_{filter_str}_full.csv"

        output_file = dirname + "/" + new_basename
        df_concat = pd.concat(
            [pd.read_csv(f, index_col=0) for f in csv_files], ignore_index=False
        )
        df_concat.to_csv(output_file)
        logging.info(f"Created {filter_str} condensed file: {output_file}")
    except IndexError:
        logging.error(f"No csv files found for filter: {filter_path}")
    except ValueError:
        logging.error(f"No values found for filter: {filter_path}")

    return output_file


def bulk_load_postgres(
    table: str = None,
    input_file: str = None,
    ref_sql: str = "DML/bulk_load_home_harvest.sql",
) -> None:
    # Read in DML file
    with open(ref_sql) as myfile:
        raw_query = " \n".join(line.rstrip() for line in myfile)
    upload_query = raw_query.format(table=table, filepath=input_file)
    logging.info(f"upload query: {upload_query}")

    # Establish connection
    conn = psycopg2.connect(
        database="homeharvest",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )

    # Execute Query
    cur = conn.cursor()
    cur.execute(upload_query)

    # Close cursor and connection
    cur.close()
    conn.close()


def update_csv(update_df: pd.DataFrame = None, current_file: str = None) -> None:
    current_file = current_file.replace('~',os.path.expanduser('~')) 
    if os.path.exists(current_file):
        current = pd.read_csv(current_file, header=0, index_col=False)
        df_concat = pd.concat([current, update_df], ignore_index=False).drop_duplicates()
        df_concat.to_csv(current_file, index=False)
    else:
        logging.debug(f"Could not find preexisting file: {current_file}")
        update_df.to_csv(current_file, index=False)


def split_extra_columns(
    df: pd.DataFrame = None, 
    index_col: List[str] = ["property_url"], 
    split_csv: str = "", 
    columns: List[str] = ["primary_photo","alt_photos"]
) -> Tuple:
    
    all_col = index_col + columns
    extra = df[all_col].copy(deep=True)
    out_df = df.drop(columns=columns).copy(deep=True)

    # extra.set_index(index_col, inplace=True)
    # out_df.set_index(index_col, inplace=True)

    if split_csv:
        update_csv(update_df=extra, current_file=split_csv)

    return out_df, extra

def full_load(
    path: str = "data/",
    years: list = ["2020", "2021", "2022", "2023", "2024"],
    quarters: List[str] = ["Q1", "Q2", "Q3", "Q4"],
    location_prefix: str = None,
    locations: List[str] = None,
    radius: int = 100,
) -> None:
    # Input Variables
    radius = 100
    split_csv = f"{path}{location_prefix}_photos.csv"

    for _, year in enumerate(years):
        for quarter in quarters:
            logging.debug(f"Processing quarter: {quarter}")
            if quarter == "Q1":
                date_from = f"{year}-01-01"
                date_to = f"{year}-03-31"
            elif quarter == "Q2":
                date_from = f"{year}-04-01"
                date_to = f"{year}-06-30"
            elif quarter == "Q3":
                date_from = f"{year}-07-01"
                date_to = f"{year}-09-30"
            elif quarter == "Q4":
                date_from = f"{year}-10-01"
                date_to = f"{year}-12-31"
            else:
                logging.error(f"Unable to process quarter: {quarter}")

            for location in locations:
                logging.info(f"Processing {location} for {quarter}_{year}")
                try:
                    ## Process Sold Properties
                    if "sold" not in locals():
                        sold = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="sold",
                            date_from=date_from,
                            date_to=date_to,
                        )
                    else:
                        df = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="sold",
                            date_from=date_from,
                            date_to=date_to,
                        )
                        sold = pd.concat([sold, df])

                    if not sold.empty:
                        sold,_ = split_extra_columns(df=sold, split_csv=split_csv)
                        current_file = f"{path}{location_prefix}_{quarter}_{year}_sold.csv"
                        update_csv(update_df=sold, current_file=current_file)

                    ## Process Selling Properties
                    if "selling" not in locals():
                        selling = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="for_sale",
                            date_from=date_from,
                            date_to=date_to,
                        )
                    else:
                        df = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="for_sale",
                            date_from=date_from,
                            date_to=date_to,
                        )
                        selling = pd.concat([selling, df])

                    if not selling.empty:
                        selling,_ = split_extra_columns(df=selling, split_csv=split_csv)
                        current_file = f"{path}{location_prefix}_{quarter}_{year}_selling.csv"
                        update_csv(update_df=selling, current_file=current_file)

                    ## Process Renting Properties
                    if "renting" not in locals():
                        renting = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="for_rent",
                            date_from=date_from,
                            date_to=date_to,
                        )
                    else:
                        df = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="for_rent",
                            date_from=date_from,
                            date_to=date_to,
                        )
                        renting = pd.concat([renting, df])

                    if not renting.empty:
                        renting,_ = split_extra_columns(df=renting, split_csv=split_csv)
                        current_file = f"{path}{location_prefix}_{quarter}_{year}_renting.csv"
                        update_csv(update_df=renting, current_file=current_file)

                    ## Process Pending Properties
                    if "pending" not in locals():
                        pending = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="pending",
                            date_from=date_from,
                            date_to=date_to,
                        )
                    else:
                        df = scrape_property(
                            radius=radius,
                            location=location,
                            listing_type="pending",
                            date_from=date_from,
                            date_to=date_to,
                        )
                        pending = pd.concat([pending, df])

                    if not pending.empty:
                        pending,_ = split_extra_columns(df=pending, split_csv=split_csv)
                        current_file = f"{path}{location_prefix}_{quarter}_{year}_pending.csv"
                        update_csv(update_df=pending, current_file=current_file)

                except AttributeError as e:
                    logging.error(e)
                except requests.exceptions.HTTPError as e:
                    logging.error(e)
                    logging.error(f"Timeout, retrying after 20 minutes")
                    time.sleep(60 * 20)
                except Exception as e:
                    logging.error(e)
                finally:
                    logging.info(
                        f"COMPLETED processing {location} for {quarter}_{year} - Sold: {len(sold)}, Selling: {len(selling)}, Pending: {len(pending)}, Renting: {len(renting)}"
                    )
                    del sold, selling, renting, pending


def main():
    input_path = "~/code/rentinvest/data/"
    file_types = ["sold", "selling", "pending", "renting"]
    locations = [
        "Oklahoma City, OK",
        "Seward, OK",
        "Guthrie, OK",
        "Edmond, OK",
        "Norman, OK",
        "Moore, OK",
        "Yukon, OK",
        "Perry, OK",
        "Stillwater, OK",
        "Perkins, OK",
        "Langston, OK",
        "Hennessey, OK",
        "Kingfisher, OK",
    ]

    ## Extract data from API
    full_load(
        path=input_path, 
        years=["2020", "2021", "2022", "2023", "2024"],
        quarters=["Q1", "Q2", "Q3", "Q4"],
        location_prefix="OKC",
        locations=locations,   
    )

    ## Transform / Load Data
    for extract_type in file_types:
        # Generate a single file for each type
        exec(
            f"full_{extract_type}_file = condense_files(input_path=input_path, filter_str='{extract_type}')"
        )

        # Load type data to postgres table
        # exec(f"bulk_load_postgres(table='{extract_type}', input_file=full_{extract_type}_file, ref_sql='DML/bulk_load_home_harvest.sql')")


if __name__ == "__main__":
    main()
