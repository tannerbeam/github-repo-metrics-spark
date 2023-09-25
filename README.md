# GitHub DevRel Metrics
Databricks notebooks workflow for creating structured tables in `github` database from GitHub API. 

### Files in Repo
- **`repo_utilities.py`** - utility module for repo-specific helper functions. 
- **`task_github_comments`** - use GitHub API to get issue comments
- **`task_github_contributors`** - use GitHub API to get external contributors
- **`task_github_issues`** - use GitHub API to get issues 
- **`task_github_pulls`** - use GitHub API to get pull requests
- **`task_github_releases`** - use GitHub API to get GX releases (aka versions)
- **`task_github_reviews`** - use GitHub API to get pull request reviews


### Workflow Details 
[See workflow details in Databricks](https://dbc-4b7bae80-92cc.cloud.databricks.com/?o=2527665687246383#job/969395416232838)

- **name:** `github_api` 
- **freq:** daily 
- **at:** UTC+00:00
- **runtime:** ~10min
- **backilled_from:** 2017 Q2


### Tables
All tables are in the `github` database in Databricks SQL. None of the tables are partitioned because it's not much data.

To see table schemas, you can use the left sidebar in Databricks: 
```
Data --> github --> table_name
```

Or if you're working in a notebook: 
```
%sql describe(github.table_name)  -- in a sql cell
```

```
spark.sql("describe github.table_name") # in a python cell
```

#### Table Names
- **`comments`**
- **`contributors`**
- **`contributors_weekly_activity`**
- **`issues`**
- **`pulls`**
- **`releases`**
- **`reviews`**


