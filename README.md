# GitHub DevRel Metrics
Databricks notebooks workflow for creating structured tables in `github` database from GitHub API. 

### Files in Repo
- `repo_init.py` - utility module for all repos. Use `repo_init.start()` at the beginning of a notebook to initialize a notebook.
- **`repo_utilities.py`** - utility module for repo-specific helper functions. 
- **`task_github_contributors`** - use GitHub API to get external contributors
- **`task_github_issues`** - use GitHub API to get issues 
- **`task_github_pulls`** - use GitHub API to get pull requests
- **`task_github_comments`** - use GitHub API to get issue comments
- **`task_github_reviews`** - use GitHub API to get pull request reviews


### Workflow Details 
(I will add more to this section when I schedule the workflow)


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
- **`contributors`**
- **`contributors_weekly_activity`**
- **`issues`**
- **`pulls`**
- **`comments`**
- **`reviews`**


