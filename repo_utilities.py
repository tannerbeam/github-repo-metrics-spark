from dataclasses import dataclass
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import datetime
import json
import os
import numpy as np
import pandas as pd
import pickle
import re
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


@dataclass
class GitUrl:
    name: str
    url: str


api = "https://api.github.com"
repo = f"{api}/repos/great-expectations/great_expectations"
gxl_members = GitUrl("gxl_members", f"{api}/orgs/greatexpectationslabs/members")
gx_members = GitUrl("gx_members", f"{api}/orgs/great-expectations/members")
contributors = GitUrl("contributors", f"{repo}/stats/contributors")
collaborators = GitUrl("collaborators", f"{repo}/collaborators")
pulls = GitUrl("pulls", f"{repo}/pulls")
issues = GitUrl("issues", f"{repo}/issues")
releases = GitUrl("releases", f"{repo}/releases")
git_url_names = [
    gu.name
    for gu in [gxl_members, gx_members, contributors, collaborators, pulls, issues, releases]
]


def get_default_creds() -> tuple:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    git_user = "tannerbeam"
    git_token = dbutils.secrets.get(
        scope="analytics_pipeline", key="tanner_github_api"
    )
    return (git_user, git_token)


def send_request_to_github_api(which_url, return_as_json=True, **kwargs):
    """
    helper function for non-paginated github api request
    """
    if which_url in git_url_names:
        url = eval(which_url).url
    else:
        url = which_url

    git_auth_tuple = get_default_creds()
    headers = {"Accept": "application/vnd.github.raw+json"}
    git_auth_tuple = get_default_creds()

    if "params" in kwargs:
        params = kwargs["params"]
        assert isinstance(params, dict), "'params' must be a dict."
        query_params = params
    else:
        query_params = {}
        
    retry_strategy = Retry(
    total=10,
    backoff_factor=5.0,
    status_forcelist=[202, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    response = http.get(
        url, params=query_params, auth=git_auth_tuple, headers=headers
    )

    if not response.status_code == 200:
        msg = f"Response failed with status code {response.status_code}"
        raise Exception(msg)
        
    elif not return_as_json:
        return response
    else:
        return response.json()


def get_core_team_members():
    response_gxl = send_request_to_github_api("gxl_members")
    response_gx = send_request_to_github_api("gx_members")
    exclude = [r["login"] for r in response_gxl + response_gx]
    also_exclude = [
        "jpcampb2",
        "spbail-ge",
        "spbail",
        "Aylr",
        "dependabot[bot]",
        "gilpasternak35",
        "snyk-bot",
        "dependabot",
        "Super-Tanner"
    ]
    return list(set(exclude + also_exclude))


# get a list of core team members
core_team = get_core_team_members()


def dt_converter(dt_string):
    if dt_string is None:
        return dt_string
    else:
        return datetime.datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%SZ")


def is_core_team_member(r, use_author = False):
    if not use_author:
        if r["user"]["login"] in core_team:
            return True
        else:
            return False
    else:
        if r["author"]["login"] in core_team:
            return True
        else:
            return False

        
def is_bot(r):
    if r["user"]["type"] == "Bot":
        return True
    else:
        return False


def is_closed(r):
    if r["state"] == "closed":
        return True
    else:
        return False


def get_pulls_dataframe(pull_history, core_team=core_team):
    """
    filter pr records and get selected fields
    """

    lst = []
    for p in pull_history:
        if p["draft"]:
            pass
        else:
            lst.append(
                {
                    "pull_number": p["number"],
                    "user_id": str(p["user"]["id"]),
                    "user_is_bot": is_bot(p),
                    "user_is_core": is_core_team_member(p),
                    "base_ref": p["base"]["ref"],
                    "head_ref": p["head"]["ref"],
                    "created_ts": p["created_at"],
                    "merged_ts": p["merged_at"],
                    "closed_ts": p["closed_at"],
                    "is_closed": is_closed(p),
                }
            )

    df = pd.DataFrame(lst)

    df["created_ts"] = df["created_ts"].apply(dt_converter)
    df["merged_ts"] = df["merged_ts"].apply(dt_converter)
    df["closed_ts"] = df["closed_ts"].apply(dt_converter)

    return df


def get_release_history():
    history = []
    page_counter = 1
    last_page = False
    query_params = {"page": "1", "per_page": 100}
    while not last_page:
        r = send_request_to_github_api("releases", False, params=query_params)
        history.extend(r.json())
        if "next" in r.links:
            page_counter += 1
            query_params["page"] = str(page_counter)
        else:
            last_page = True
            print(f"Finished on page {page_counter}")
    return history


def get_releases_dataframe(release_history): 
    df = pd.DataFrame(
    [
        {
            "version": r["tag_name"], 
            "name": r["name"], 
             "release_dt": dt_converter(r["published_at"])
        } for r in release_history
    ]
    )
    df["release_dt"] = df.release_dt.apply(lambda x: x.date())
    df["version"] = np.where(
        df.version.str.startswith("v"), 
        df.version.str.replace("v",""), 
        df.version
    )
    df["version"] = np.where(
        df.version.str.contains("[a-f]+"), 
        df.name,
        df.version
    )
    df = df.query("~version.str.contains('[a-z]')").query("~release_dt.isnull()")
    pat = r"^([0-1])\.(\d{1,2})\.(\d{1,2})"
    matches = df.version.apply(lambda x: re.search(pat, x))
    [m.group(1) for m in matches]
    df["major"] = [m.group(1) for m in matches]
    df["minor"] = [m.group(2) for m in matches]
    df["patch"] = [m.group(3) for m in matches]
    cols = df.columns[df.columns != "name"]
    df = df.filter(items = cols)
    df.sort_values(
        by = ["major", "minor", "patch"], 
        ascending = [False, False, False], 
        inplace = True
    )
    return df


def get_pulls_history(**kwargs):
    """
    get entire history of pull requests
    """
    pull_history = []
    
    if not "params" in kwargs:
        query_params = get_pulls_query_params(**kwargs)
    else:
        query_params = kwargs["params"]
        
    page_counter = 1
    last_page = False
    pulls = []
    
    while not last_page:
        r = send_request_to_github_api("pulls", False, params=query_params)
        pull_history.extend(r.json())
        if "next" in r.links:
            page_counter += 1
            query_params["page"] = str(page_counter)
        else:
            last_page = True
            print(f"Finished on page {page_counter}")
    return pull_history


def get_pulls_query_params(**kwargs):
    """
    helper to get and update defaults for making a github /pull api request
    """
    default_params = {
        "state": "all",
        "sort": "created",
        "direction": "desc",
        "per_page": "100",
        "page": "1",
    }
    allowed_params = ["state", "head", "base", "sort", "direction", "per_page"]

    state_values = ["open", "closed", "all"]
    sort_values = ["created", "updated", "popularity", "long-running"]
    direction_values = ["asc", "desc"]

    if not "params" in kwargs:
        return default_params
    else:
        query_params = default_params.copy()
        for k, v in kwargs["params"].items():
            if not k in allowed_params:
                raise ValueError(
                    f"Additional item {kwargs.get(k)} not in list of accepted query params."
                )
            elif k == "state":
                assert (
                    kwargs["params"].get(k) in state_values
                ), f"{k} must be one of {state_values}."
            elif k == "sort":
                assert (
                    kwargs["params"].get(k) in sort_values
                ), f"{k} must be one of {sort_values}."
            elif k == "direction":
                assert (
                    kwargs["params"].get(k) in direction_values
                ), f"{k} must be one of {direction_values}."
            elif k == "per_page":
                assert int(kwargs["params"].get(k)) in range(
                    1, 101
                ), f"{k} must be between 1 and 100."
            query_params.update({k: v})
        return query_params


def get_contributors_dataframe(responses, core_team=core_team):
    """
    parse contributor json data returned from api request and dump into pandas dataframe
    """
    lst = []

    for r in responses:
        weeks = [datetime.datetime.fromtimestamp(d["w"]).date() for d in r["weeks"]]
        payload = {
            "user_name": ([f"{r['author']['login']}"] * len(weeks)),
            "user_id": ([f"{r['author']['id']}"] * len(weeks)),
            "user_is_core": is_core_team_member(r, use_author = True),
            "week": weeks,
            "adds": [d["a"] for d in r["weeks"]],
            "deletes": [d["d"] for d in r["weeks"]],
            "commits": [d["c"] for d in r["weeks"]],
            "events": [(d["a"] + d["d"] + d["c"]) for d in r["weeks"]],
        }
        lst.append(payload)

    df = pd.DataFrame(lst).apply(pd.Series.explode).set_index("user_id")
    df.sort_values(["user_id", "week"], inplace=True)
    df = df.reset_index()
    return df


def get_contributors_users_dataframe(df):
    """
    get a dataframe grouped by user_id with all-time summary metrics
    """

    # dataframe grouped by user and week
    df_contrib_user_week = df.groupby(["user_id", "week"]).agg(
        user_name=("user_name", "max"),
        user_is_core=("user_is_core", "max"),
        adds=("adds", "sum"),
        deletes=("deletes", "sum"),
        commits=("commits", "sum"),
        events=("events", "sum"),
    )

    # dataframe grouped by user (all-time)
    df_user = df_contrib_user_week.groupby("user_id").agg(
        user_name=("user_name", "max"),
        user_is_core=("user_is_core", "max"),
        adds=("adds", "sum"),
        deletes=("deletes", "sum"),
        commits=("commits", "sum"),
        events=("events", "sum"),
    )

    # series with user's first week of activity
    df_user["first_week"] = (
        df_contrib_user_week.groupby("user_id")
        .apply(lambda x: x.events.gt(0).idxmax())
        .apply(lambda x: pd.to_datetime(x[1]).date())
    )

    # series with user's last week of activity
    df_user["last_week"] = (
        df_contrib_user_week.sort_values(["user_id", "week"], ascending=[True, False])
        .groupby("user_id")
        .apply(lambda x: x.events.gt(0).idxmax())
        .apply(lambda x: pd.to_datetime(x[1]).date())
    )

    # series with user's count of weeks active
    df_user["active_weeks"] = df_contrib_user_week.groupby("user_id").apply(
        lambda x: x.events.gt(0).sum()
    )

    # series with user's weeks lapsed between first and last activity
    df_user["span_weeks"] = (
        pd.to_datetime(df_user["last_week"]) - pd.to_datetime(df_user["first_week"])
    ).apply(lambda x: 1 if (x.days / 7 == 0) else int(x.days / 7))

    return df_user.reset_index()


def get_contributors_weeks_dataframe(df, df_to_merge):
    """
    get a dataframe grouped by week and user
    """

    # dataframe grouped by user and week
    df_contrib_week = (
        df.groupby(["week", "user_id"])
        .agg(
            adds=("adds", "sum"),
            deletes=("deletes", "sum"),
            commits=("commits", "sum"),
            events=("events", "sum"),
        )
        .reset_index()
    )

    # merge with dataframe that has user's first_week info
    df_to_merge = df_to_merge.filter(items=["user_id", "first_week"])
    df_merged = df_contrib_week.merge(
        df_to_merge, left_on="user_id", right_on="user_id"
    )
    df_out = df_merged[df_merged.week >= df_merged.first_week]
    keep = df_out.columns[df_out.columns != "first_week"]
    df_out = df_out.filter(items = keep, axis = 1)
    df_out = df_out.sort_values(
        by = ["week", "commits"], 
        ascending = [False, False]
    ).reset_index(drop = True)
    
    return df_out


def get_issues_query_params(**kwargs):
    """
    helper to get and update defaults for making a github /issues api request
    """
    default_params = {
        "state": "all",
        "sort": "created",
        "direction": "desc",
        "per_page": "100",
        "page": "1",
    }
    allowed_params = [
        "filter",
        "state",
        "labels",
        "sort",
        "direction",
        "since",
        "collab",
        "orgs",
        "owned",
        "pulls",
        "per_page",
    ]

    state_values = ["open", "closed", "all"]
    sort_values = ["created", "updated", "comments"]
    direction_values = ["asc", "desc"]
    filter_values = ["assigned", "created", "mentioned", "subscribed", "repos", "all"]

    if not "params" in kwargs:
        return default_params
    else:
        query_params = default_params.copy()
        for k, v in kwargs["params"].items():
            if not k in allowed_params:
                raise ValueError(
                    f"Additional item {kwargs.get(k)} not in list of accepted query params."
                )
            elif k == "state":
                assert (
                    kwargs["params"].get(k) in state_values
                ), f"{k} must be one of {state_values}."
            elif k == "sort":
                assert (
                    kwargs["params"].get(k) in sort_values
                ), f"{k} must be one of {sort_values}."
            elif k == "direction":
                assert (
                    kwargs["params"].get(k) in direction_values
                ), f"{k} must be one of {direction_values}."
            elif k == "per_page":
                assert int(kwargs["params"].get(k)) in range(
                    1, 101
                ), f"{k} must be between 1 and 100."
            elif k == "filter":
                assert (
                    kwargs["params"].get(k) in filter_values
                ), f"{k} must be one of {filter_values}."
            elif k == "since":
                if not kwargs["params"].get(k).endswith("Z"):
                    v = f"{v}T00:00:00Z"
            elif k == "pulls":
                assert (
                    kwargs["params"].get(k) in ["true", "false"]
                ), f"{k} must be 'true' or 'false'."
            query_params.update({k: v})
        return query_params


def get_issues_history(**kwargs):
    """
    get entire history of issues
    """
    history = []
    if not "params" in kwargs:
        query_params = get_pulls_query_params(**kwargs)
    else:
        query_params = kwargs["params"]
    page_counter = 1
    last_page = False
    pulls = []
    while not last_page:
        r = send_request_to_github_api("issues", False, params=query_params)
        history.extend(r.json())
        if "next" in r.links:
            page_counter += 1
            query_params["page"] = str(page_counter)
        else:
            last_page = True
            print(f"Finished on page {page_counter}")
    return history


def get_issues_dataframe(issues_history, core_team=core_team):
    """
    filter issues records and get selected fields
    """

    lst = []

    for r in issues_history:
        if "draft" in r:
            if r["draft"]:
                pass
        if "pull_request" in r:
            is_pr = True
        else:
            is_pr = False
        lst.append(
            {
                "issue_number": r["number"],
                "is_pull_request": is_pr,
                "title": r["title"],
                "user_id": r["user"]["id"],
                "user_name": r["user"]["login"],
                "user_is_bot": is_bot(r),
                "user_is_core": is_core_team_member(r),
                "labels": [l["name"] for l in r["labels"]],
                "comment_count": r["comments"],
                "reaction_count": r["reactions"]["total_count"],
                "created_ts": r["created_at"],
                "updated_ts": r["updated_at"],
                "closed_ts": r["closed_at"],
                "is_closed": is_closed(r),
            }
        )

    return pd.DataFrame(lst)


def get_comments_history(get_full_history=False):
    """
    get selected reviews info from api calls to issues/{issue_number}/comments
    """

    spark = SparkSession.builder.getOrCreate()

    if get_full_history:
        sql = """ 
            select distinct issue_number 
            from github.issues 
        """
        issues = [r[0] for r in spark.sql(sql).collect()]
    else:
        sql = """
            select distinct issue_number 
            from github.issues 
            where updated_ts >= (select max(updated_ts) from github.comments)
        """
        issues = [r[0] for r in spark.sql(sql).collect()]
        
    lst_outer = []
    for i in issues:
        comments_url = f"{repo}/issues/{i}/comments"
        response = send_request_to_github_api(comments_url, True)
        lst_inner = []
        for c in response:
            if is_bot(c):
                pass
            else:
                lst_inner.append(
                    {
                        "comment_id": c["id"],
                        "issue_number": i,
                        "user_id": c["user"]["id"],
                        "user_name": c["user"]["login"],
                        "user_is_core": is_core_team_member(c),
                        "created_ts": c["created_at"],
                        "updated_ts": c["updated_at"],
                        "body": c["body"],
                    }
                )

            lst_outer.append(lst_inner)
    return lst_outer


def get_comments_dataframe(comments_history):
    """
    unpack comments in comments_history api reponse and put into dataframe
    """
    payload = []
    for lst in comments_history:
        for comment in lst:
            unpacked = {**comment}
            payload.append(unpacked)

    return pd.DataFrame(payload)


def get_reviews_history(get_full_history=False):
    """
    get selected reviews info from api calls to pulls/{pull_number}/reviews
    """

    spark = SparkSession.builder.getOrCreate()

    if get_full_history:
        sql = """ 
            select distinct pull_number 
            from github.pulls
        """
        issues = [r[0] for r in spark.sql(sql).collect()]
        
    else:
        sql = """
            select distinct issue_number 
            from github.issues 
            where is_pull_request 
            and updated_ts >= (select max(submitted_ts) from github.reviews)
        """
        issues = [r[0] for r in spark.sql(sql).collect()]

    lst_outer = []
    for i in issues:
        reviews_url = f"{repo}/pulls/{i}/reviews"
        response = send_request_to_github_api(reviews_url, True)
        lst_inner = []
        for r in response:
            lst_inner.append(
                {
                    "review_id": r["id"],
                    "pull_number": i,
                    "user_id": r["user"]["id"],
                    "user_name": r["user"]["login"],
                    "user_is_core": is_core_team_member(r),
                    "status": r["state"],
                    "submitted_ts": dt_converter(r["submitted_at"])
                }
            )
            lst_outer.append(lst_inner)

    return lst_outer


def get_reviews_dataframe(reviews_history):
    """
    unpack reviews in reviews_history api reponse and put into dataframe
    """

    payload = []
    for lst in reviews_history:
        for review in lst:
            unpacked = {**review}
            payload.append(unpacked)

    return pd.DataFrame(payload)
