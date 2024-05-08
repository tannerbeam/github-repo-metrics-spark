from etl.spark.etl_config import get_config, EtlConfigError
from etl.spark import spark_utils

import datetime
from time import sleep
import requests
from requests.exceptions import HTTPError, RequestException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from typing import Optional
from pyspark.errors import IllegalArgumentException

config = get_config()


def get_github_api_key():
    try:
        spark_utils.get_secret(config.github_secret_scope, config.github_secret_key)
    except IllegalArgumentException:
        raise EtlConfigError(
            """
            Missing or incorrect databricks secret scope and/or key in repo config.
            Secrets are accessed with dbutils.secrets.get(scope='scope_name', key='key_name').
            Check config dict in config/repo.py to ensure k:v pairs like
                {
                    'github_secret_scope': 'scope_name', 
                    'github_secret_key': 'key_name',
                }
            """
        )
    else:
        return spark_utils.get_secret(config.github_secret_scope, config.github_secret_key)


def list_github_api_versions() -> list[str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {get_github_api_key()}",
    }
    request_url = "https://api.github.com/versions"
    response = requests.get(request_url, headers=headers)
    return sorted(response.json(), reverse=True)


def get_latest_github_api_version() -> str:
    return list_github_api_versions()[0]


def default_get_headers() -> dict[str]:
    """
    Set default headers for http_get request
    """
    api_key: str = get_github_api_key()

    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_rate_limits(core_only: bool = True) -> dict:
    """
    Get status from /rate_limit endpoint
    Only return core rate limits if core_only==True.
    """
    url = "https://api.github.com/rate_limit"
    response_json = requests.get(url, headers=default_get_headers()).json()
    return (
        response_json.get("resources").get("core")
        if core_only == True
        else response_json
    )


def sleepy_time(
    remaining: int, trigger_from: int = 50, step: int = 5, backoff_factor: float = 3
):
    """
    Force increasing sleep interval as remaining requests decreases.
    Args:
        - remaining: # of requests remaining (per API call)
        - trigger_from: start triggering intervals from this # of requests remaining
        - step: trigger intervals in steps of
        - backoff_factor: probably good in the 1-3 range, but YMMV
    """
    inverse = trigger_from - remaining
    if (remaining <= trigger_from) and (remaining % step == 0):
        naptime_seconds = backoff_factor * (2 ** (inverse / step - 1))
        print(
            f"{remaining} requests remain. Taking a nap for ~{round(naptime_seconds/60, 8)} min..."
        )
        sleep(naptime_seconds)


def check_core_rate_limit(core_only: bool = True):
    """
    Raise exception if requests depleted.
    Otherwise print warning if threshold exceeded.
    """
    rl = get_rate_limits(core_only=core_only)

    limit = rl.get("limit")
    remaining = rl.get("remaining")

    now_ts = datetime.datetime.now().timestamp()
    reset_ts = rl.get("reset")
    reset_minutes = round((reset_ts - now_ts) / 60, 0)

    if remaining == 0:
        raise RequestException(
            f"Core API rate limit of {limit}/hour exceeded. Reset occurs in {reset_minutes} minutes."
        )
    else:
        sleepy_time(remaining)


def http_get(
    url: str, headers: Optional[dict[str]] = None, params: Optional[dict[str]] = None
):
    """
    Generic paginated get request.
    Return list[json] if paginated and json otherwise.
    Check rate limit status before issuing request.
    """
    if not headers:
        headers = default_get_headers()

    if not params:
        params = {}

    if not isinstance(params, dict):
        raise ValueError("Request params must be a dict with {'param': 'value', ...}")

    retry_strategy = Retry(
        total=10, backoff_factor=5.0, status_forcelist=[202, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    responses = []
    page_counter = 1
    last_page = False

    while not last_page:
        check_core_rate_limit()
        response = session.get(url, params=params, headers=headers)
        responses.extend(response.json())
        if "next" in response.links:
            page_counter += 1
            params["page"] = str(page_counter)
        else:
            last_page = True

    return responses if page_counter > 1 else response.json()