from datetime import datetime
from typing import Optional


def github_timestamp_string(input_string: str, input_format: Optional[str] = None):
    """
    Convert a standard timestamp string to Github API format.
    Default input format is "%Y-%m-%d %H:%M:%S"
    """
    assert isinstance(input_string, str), "input string to be converted must be of string type."

    if input_format is None:
        input_format = "%Y-%m-%d %H:%M:%S"

    return datetime.strptime(input_string, input_format).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_date_string() -> str:
    return datetime.now().date().strftime("%Y-%m-%d")


def now_timestamp_string() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def unix_to_iso_string(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts).date().strftime("%Y-%m-%d")


def timestamp_string_from_datetime(input_datetime: datetime, github_format: bool = False):
    """
    Convert a datetime.datetime object to timestamp string
    """
    assert isinstance(input_datetime, datetime), "input datetime to be converted must be of datetime type."

    ts = input_datetime.strftime("%Y-%m-%d %H:%M:%S")
    github_ts = github_timestamp_string(ts)

    return ts if not github_format else github_ts


    