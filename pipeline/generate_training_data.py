import json
import numpy as np
import pandas as pd

from typing import Optional
from pathlib import Path

from constants import DB_NAME
from attribution_orchestration import _connect_to_sqlite_db, _read_sql_file
from logger import logger


def get_customer_journey_df(
    sql_file: str = "customer_journeys.sql",
    base_path: Optional[Path] = Path("sql/ingestion/"),
    params: dict = None,
) -> pd.DataFrame:
    try:
        file_path = base_path / sql_file
        query = _read_sql_file(file_path)

        with _connect_to_sqlite_db(DB_NAME) as conn:
            logger.info("Running query ...")
            return pd.read_sql_query(
                query,
                con=conn,
                params=params,
            )
    except FileNotFoundError:
        logger.error(f"Error: SQL file not found in '{file_path}'")
        raise


def create_test_dataset(df: pd.DataFrame, sample_fraction=0.25) -> pd.DataFrame:
    """
    Creates a test dataset by sampling complete customer journeys.

    This approach ensures we maintain the natural relationships between:
    - Multiple touchpoints in a single customer journey
    - Engagement patterns across different channels
    - Time sequences between interactions
    - The progression from first touch to conversion

    Args:
        df: Original DataFrame with all customer journeys
        sample_size: Number of conversion journeys to sample

    Returns:
        DataFrame containing complete journeys for the sampled conversions
    """
    # Get unique conversion IDs
    all_conversions = df["conversion_id"].unique()

    # Sample conversion IDs while respecting the limit
    sample_size = int(len(all_conversions) * sample_fraction)
    sampled_conversions = np.random.choice(
        all_conversions, size=sample_size, replace=False
    )
    logger.info(
        f"Creating test data of size {sample_size} from a total of {len(all_conversions)} conversion ids"
    )

    # Get all touchpoints for these conversions and sort
    sampled_journeys = df[df["conversion_id"].isin(sampled_conversions)].copy()
    sampled_journeys = sampled_journeys.sort_values(["conversion_id", "timestamp"])

    return sampled_journeys


def write_test_data_file(
    df,
    file_name: str = "training_data.json",
    base_path: Path = Path("data/training/"),
) -> None:
    training_data_path = base_path / file_name
    test_data = df[
        [
            "conversion_id",
            "session_id",
            "timestamp",
            "channel_label",
            "holder_engagement",
            "closer_engagement",
            "conversion",
            "impression_interaction",
        ]
    ].to_dict("records")

    logger.info(f"Writing test data to {training_data_path}")
    with open(training_data_path, "w") as f:
        json.dump(test_data, f, indent=2)
    logger.info("Done!")


def main():
    params = {
        "start_date": None,
        "end_date": None,
    }
    df = get_customer_journey_df(params=params)
    df_test = create_test_dataset(df)
    write_test_data_file(df_test)


if __name__ == "__main__":
    main()
