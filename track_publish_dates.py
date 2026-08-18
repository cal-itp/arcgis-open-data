"""
Grab all the operators by service date from
saved scheduled_trips tables from GCS.

Create a yaml that tells us the most recent
date available for each operator (schedule_gtfs_dataset_name).

Still used in speedmaps and hqta as of Dec 16 2025
"""

import datetime
from pathlib import Path
from typing import Union

import pandas as pd
import pyaml  # use pyaml because it gets us prettier indents than yaml
# from segment_speed_utils import time_series_utils
from shared_utils import gtfs_utils_v2, publish_utils, rt_dates

from functools import cache

from calitp_data_analysis.gcs_pandas import GCSPandas
import dask.dataframe as dd

@cache
def gcs_pandas():
    return GCSPandas()


def export_results_yml(df: pd.DataFrame, export_yaml: Union[str, Path]):
    """
    Save out our results from df.
    Convert df into a dictionary and save out dictionary results as yaml.
    """
    # TODO: check this list manually and there will be some
    # operator names that have more recent names that we are keeping,
    # so we can remove these from our yaml
    exclude_me = [
        "Flex",
        "Anaheim Resort", # ceased operation
        "Merced Schedule", # feed replaced
        "Santa Cruz Schedule", # feed replaced
        "Cerritos on Wheels", # ceased operation
        "Clovis Schedule", # feed replaced
        "Redwood Coast Schedule", # feed replaced
        "County Express Schedule", # feed replaced
        "Redding Schedule", # feed replaced
    ]

    df2 = df.copy()

    for exclude_word in exclude_me:

        df2 = df2[~df2.name.str.contains(exclude_word)]

    # yaml export can have date as string
    # but yaml safe_load will automatically parse as datetime again
    my_dict = {**{date_key: df2[df2.service_date == date_key].name.tolist() for date_key in df2.service_date.unique()}}

    # sort_keys=False to prevent alphabetical sort (earliest date first)
    # because we want to main our results and yaml with most recent date first
    output = pyaml.dump(my_dict, sort_keys=False)

    with open(export_yaml, "w") as f:
        f.write(output)

    print(f"{export_yaml} exported")

    return

def import_df_func(path, one_date, **kwargs):
    '''
    adapt/simplify from https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/dask_utils.py
    '''
    df = gcs_pandas().read_parquet(f'{path}_{one_date}.parquet', **kwargs).drop_duplicates()
    df = df = df.assign(service_date=pd.to_datetime(one_date))
    return df


if __name__ == "__main__":

    from update_vars import (
        COMPILED_CACHED_VIEWS,
        GTFS_DATA_DICT,
    )

    TABLE = GTFS_DATA_DICT.schedule_downloads.trips

    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    date_list = rt_dates.y2026_dates

    paths = [f"{COMPILED_CACHED_VIEWS}{TABLE}" for _ in date_list]
    operators = (
        dd.from_map(import_df_func, paths, date_list,
                     filters=[[("gtfs_dataset_key", "in", public_feeds)]], columns=['name'])
        .compute()
        .drop_duplicates()
        .pipe(publish_utils.filter_to_recent_date, ["name"])
        .astype({"service_date": "str"})
        )

    current_year = str(datetime.datetime.now().year)
    assert (operators.service_date.str.contains(current_year)).any(), "must add current calendar year, see README"

    export_results_yml(operators, 'published_operators.yml')