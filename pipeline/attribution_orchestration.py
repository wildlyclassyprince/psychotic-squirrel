import argparse
import time
import json
import requests
import sqlite3
import pandas as pd

from pathlib import Path
from typing import Optional, List
from datetime import datetime

from logger import logger
from constants import (
    IHC_API_TOKEN,
    DB_NAME,
    SCHEMAS,
    INGESTION,
    REDISTRIBUTION_PARAMETER,
    CONV_TYPE_ID,
    CHUNK_SIZE,
)


def _connect_to_sqlite_db(db_name: str = DB_NAME):
    """Connect to a SQLite database."""
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except sqlite3.Error as e:
        logger.error(f"There was an error connecting to the sqlite database: {e}")
        raise


def _read_sql_file(file_path: Path) -> str:
    """Read SQL query from file."""
    try:
        logger.info(f"Reading file '{file_path}'")
        return file_path.read_text()
    except FileNotFoundError:
        logger.error(f"Error: SQL file not found in '{file_path}'")
        raise


def _purge_table(table_name: str) -> None:
    with _connect_to_sqlite_db() as conn:
        logger.info(f"Purging {table_name}")
        conn.execute("BEGIN TRANSACTION;")
        conn.execute(f"DELETE FROM {table_name};")
        conn.execute("COMMIT;")


def get_customer_journey_records(
    sql_file: str,
    base_path: Optional[Path] = Path("sql/ingestion/"),
    params: dict = None,
) -> pd.DataFrame:
    """
    Query the database and build customer journeys.
    """
    file_path = base_path / sql_file
    query = _read_sql_file(file_path)

    training_data = pd.read_json("data/training/training_data.json")

    with _connect_to_sqlite_db() as conn:
        if params and params["start_date"] and params["end_date"]:
            logger.info(
                f"Running query for records between '{params["start_date"]}' and '{params["end_date"]}'"
            )
        else:
            logger.info("Running query ...")
        journeys = pd.read_sql_query(
            query,
            con=conn,
            params=params,
        )
        logger.info("Cleaning up dataframe ...")
        final_journeys = journeys.loc[
            ~(journeys.conversion_id.isin(training_data.conversion_id))
        ]
        logger.info("Transforming dataframe into records ...")
        return final_journeys.to_dict("records")


def send_api_request(
    customer_journey_records: List[dict],
    redistribution_parameter: dict = REDISTRIBUTION_PARAMETER,
    api_key: str = IHC_API_TOKEN,
    conv_type_id: str = CONV_TYPE_ID,
):
    """
    Send IHC API request.
    """
    api_url = (
        f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}"
    )
    try:
        body = {
            "customer_journeys": customer_journey_records,
            "redistribution_parameter": redistribution_parameter,
        }
        logger.info(f"Sending request to URL: {api_url}")
        response = requests.post(
            api_url,
            data=json.dumps(body),
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
            },
        )
        if response.status_code == 200:
            return response
        elif response.status_code == 206:
            logger.error(
                f"Error: the request succeeded but there are partial errors - {response.json()}"
            )
            return None
        elif response.status_code == 400:
            logger.error(
                f"Error: request failed due to invalid input - {response.json()}"
            )
            return None
        elif response.status_code == 406:
            logger.error(
                f"Error: (in partial failures object) error in parsing customer journey - {response.json()}"
            )
            return None
        elif response.status_code == 500:
            logger.error(f"Error: response value was 'None' - {response}")
            return None
        else:
            logger.error(
                f"Unexpected status error code {response.status_code}: {response.json()}"
            )
            return None

    except requests.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON response: {str(e)}")
        raise


def process_api_response(
    response: requests.models.Response,
    table_name: str = "attribution_customer_journey",
    upsert_sql_file: str = "upsert_attribution_customer_journey.sql",
    base_path: Path = Path("sql/transformation/"),
) -> None:
    """
    Process the response from the request and write the result to the 'attribution_customer_journey' table.
    Use a temporary table to insert then perform an upsert to avoid duplicates and preserve idempotency,
    before dropping the temporary table.
    """
    # Setup
    temp_table_name = "temp_" + table_name
    upsert_sql_file = base_path / upsert_sql_file
    upsert_sql = _read_sql_file(upsert_sql_file)

    try:
        json_data = response.json()
        attributions = pd.DataFrame.from_records(json_data["value"])

        with _connect_to_sqlite_db() as conn:
            logger.info(f"Inserting records to '{temp_table_name}'")
            attributions.to_sql(
                name=temp_table_name,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.info(
                f"Performing upsert from '{temp_table_name}' into '{table_name}'"
            )
            conn.execute(upsert_sql)

            logger.info(f"Dropping '{temp_table_name}'")
            conn.execute(f"DROP TABLE {temp_table_name}")
        logger.info(
            f"Successfully wrote {attributions.shape[0]} records to table '{table_name}'"
        )
    except ValueError as e:
        logger.error(f"There was an issue processing the JSON response: {str(e)}")
        raise
    except AttributeError:
        logger.info(f"No records to process to {table_name}")
        _purge_table(table_name=table_name)


def update_schemas(
    sql_file: str,
    base_path: Optional[Path] = Path("sql/schemas/"),
    max_retries: int = 3,
    wait_seconds: int = 5,
) -> None:
    """
    Create or update table schemas before inserting data.
    """
    file_path = base_path / sql_file
    query = _read_sql_file(file_path)

    for attempt in range(max_retries):
        try:
            with _connect_to_sqlite_db() as conn:
                conn.execute(query)
                logger.info(f"Successfully updated schema file '{sql_file}'")
                return True
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                if attempt < max_retries - 1:
                    logger.info(
                        f"Attempt {attempt + 1}, retrying in {wait_seconds} seconds ..."
                    )
                    time.sleep(wait_seconds)
                    continue
                else:
                    logger.error(
                        f"Max retry attempts reached. Could not execute query in file '{file_path}'"
                    )
                    return False
            else:
                raise sqlite3.OperationalError(
                    f"Database error not related to database locking: {str(e)}"
                ) from e


def build_channel_reporting(
    report_sql_file: str = "build_report_customer_journey_channel.sql",
    base_path: Path = Path("sql/reporting/"),
):
    """
    Fill the table channel_reporting by querying the now filled four tables:
        - session_sources
        - session_costs
        - conversions
        - attribution_customer_journey
    """
    file_path = base_path / report_sql_file
    query = _read_sql_file(file_path)

    try:
        logger.info("Purge before creating new report ...")
        _purge_table(table_name="channel_reporting")

        with _connect_to_sqlite_db() as conn:
            conn.execute("BEGIN TRANSACTION;")
            conn.execute(query)
            conn.execute("COMMIT;")
            logger.info("Successfully created channel report!")
            return True
    except sqlite3.IntegrityError as e:
        logger.error(f"A table constraint was violated: {str(e)}")
        raise
    except sqlite3.OperationalError as e:
        logger.error(f"There is a syntax error. Check your query: {str(e)}")
        raise


def export_channel_report_to_csv(
    sql_file: str = "channel_report.sql",
    base_path: Path = Path("sql/adhoc/"),
    export_base_path: Path = Path("data/exports/"),
    export_file_name: str = "channel_reporting.csv",
):
    """
    Create a .csv file of channel_reporting and add the following two columns:
        - CPO: (cost per order) showing the amount of marketing costs for the given date and channel that was spent on getting one attributed (IHC) order
        - ROAS: (return on ad spend) showing revenue earned for each Euro you spend on marketing
    """
    try:
        file_path = base_path / sql_file
        query = _read_sql_file(file_path)

        full_export_path = export_base_path / export_file_name

        with _connect_to_sqlite_db() as conn:
            logger.info("Running query ...")
            result = pd.read_sql_query(
                query,
                con=conn,
            )
            logger.info(f"Writing results to '{full_export_path}'")
            result.to_csv(
                full_export_path,
                index=False,
                header=True,
            )
            logger.info(
                f"Successfully exported results and saved in '{full_export_path}'"
            )
            return True
    except FileNotFoundError:
        logger.error(f"Error: SQL file not found in '{file_path}'")
        raise


def parse_date(date_str: str):
    """
    Validate and parse date string. Ensure it's in YYYY-MM-DD format.
    """
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"The date format is not valid: {date_str}. Use 'YYYY-MM-DD'"
        )


def get_args():
    parser = argparse.ArgumentParser(
        description="Process customer journeys with an optional date range"
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date for conversions and sessions of interest",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date for conversions and sessions of interest",
    )
    args = parser.parse_args()

    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("start-date must be before or the same as the end-date")

    return args


def main():
    args = get_args()
    params = {
        "start_date": args.start_date,
        "end_date": args.end_date,
    }
    logger.info("Getting customer journey records ...")
    customer_journey_records = get_customer_journey_records(
        sql_file=INGESTION["customer_journeys"],
        params=params,
    )
    total_records = len(customer_journey_records)
    logger.info(f"Number of customer journey records: {total_records}")
    logger.info("============================================")

    logger.info("Preparing to send requests and process responses ...")
    update_schemas(sql_file=SCHEMAS["attribution_customer_journey"])
    logger.info("============================================")

    logger.info("Starting to send requests in chunks ...")
    logger.info("Purge attribution_customer_journey before inserting data ...")
    _purge_table(table_name="attribution_customer_journey")
    if total_records > 0:
        counter = 1
        for start_idx in range(0, total_records, CHUNK_SIZE):
            logger.info(f"Starting to process chunk {counter}")
            # Get a chunk of customer journeys, up to the chunk_size records
            end_idx = min(start_idx + CHUNK_SIZE, total_records)
            customer_journey_chunk = customer_journey_records[start_idx:end_idx]
            response = send_api_request(customer_journey_records=customer_journey_chunk)
            process_api_response(response=response)
            logger.info(f"Finished processing chunk {counter}")
            logger.info("============================================")
            counter += 1
    else:
        process_api_response(response=None)
    logger.info("Finished processing request responses.")
    logger.info("============================================")

    logger.info("Preparing to create channel report ...")
    update_schemas(sql_file=SCHEMAS["channel_reporting"])
    logger.info("============================================")

    logger.info("Building channel report ...")
    build_channel_reporting()
    logger.info("============================================")

    logger.info("Exporting channel report to CSV ...")
    export_channel_report_to_csv()
    logger.info("Attribution pipeline completed successfully!")
    logger.info("============================================")


if __name__ == "__main__":
    main()
