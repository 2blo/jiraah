from config import Config
import yaml
import os
from typing import List
from jira import JIRA, Issue
from datetime import datetime
import pandas as pd
from duckdb import sql
from IPython.display import display
from dotenv import load_dotenv

load_dotenv(override=True)

pd.options.display.max_rows = 4000
pd.set_option('display.max_colwidth', None)


jira_token = os.environ["JIRA_TOKEN"]

with open(os.environ["CONFIG_PATH"], "r") as f:
    config = Config.model_validate(yaml.safe_load(f))



def _jira_client(server: str, api_token: str) -> JIRA:
    return JIRA(options={"server": server}, token_auth=api_token)


def get_issues(
    server: str, api_token: str, jql: str, max_results: int = 100
) -> List[Issue]:
    jira = _jira_client(server, api_token)
    return jira.search_issues(jql, maxResults=max_results)



def readable_date(date_str: str) -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    return str(date_obj.strftime("%Y-%m-%d"))


def readable_sprint(encoded_sprint: str) -> str:
    parameters = encoded_sprint.split(",")
    sprint_parameter = [parameter for parameter in parameters if "name=" in parameter][
        0
    ]
    return sprint_parameter.split("name=")[1]


feature_keys = ",".join(f'"{feature}"' for feature in config.features)
feature_link_field_id = 10702

stories_query = f"cf[{feature_link_field_id}] in ({feature_keys}) and status != Done ORDER BY updated DESC"
stories = get_issues(config.server, jira_token, stories_query, max_results=1000)


report = [
    {
        "key": issue.key,
        "points": issue.fields.customfield_10708,
        "summary": issue.fields.summary,
        "url": f"{config.server}/browse/{issue.key}",
        "issue type": issue.fields.issuetype.name,
        "parent feature": issue.fields.customfield_10702,
        "issue links": [
            {
                "key": link.outwardIssue.key,
                "summary": link.outwardIssue.fields.summary,
            }
            for link in issue.fields.issuelinks
            if hasattr(link, "outwardIssue")
        ],
        "created": readable_date(issue.fields.created),
        "updated": readable_date(issue.fields.updated),
        "sprints": [
            readable_sprint(issue_sprint)
            for issue_sprint in issue.fields.customfield_10701 or []
        ],
        "creator": issue.fields.creator.displayName,
        "reporter": issue.fields.reporter.displayName,
        "assignee": getattr(issue.fields.assignee, "displayName", None),
        "leading work group": getattr(issue.fields.customfield_14400, "value", None),
        "organisation": getattr(issue.fields.customfield_15100, "value", None),
        "organisation code": getattr(issue.fields.customfield_12803, "value", None),
        "fix versions": [version.name for version in issue.fields.fixVersions],
        "labels": issue.fields.labels,
        "description": issue.fields.description,
    }
    for issue in stories
]

with open(config.features_path, "w") as file:
    yaml.dump(report, file, sort_keys=False)

with open(config.miro_path, "r") as file:
    miro_df = pd.read_csv(file)

jira_df = pd.DataFrame(report)

print("Stories that are missing in Jira or Miro:")
sql(f"""--sql
select
    jira_df."parent feature" as jira_parent_feature,
    jira_df.key as jira_key,
    jira_df.summary as jira_summary,
    miro_df.key as miro_key,
    miro_df.summary as miro_summary,
    jira_df.url
from
    jira_df
full outer join
    miro_df
on
    miro_df.key = jira_df.key
where
    miro_df.key is null or jira_df.key is null
""").show(max_rows=100, max_width=10000)


print("Stories that have different points in Jira and Miro:")
sql(f"""--sql
select
    miro_df.key,
    jira_df.url,
    miro_df.points as miro_points,
    jira_df.points as jira_points
from
    miro_df
join
    jira_df
on
    miro_df.key = jira_df.key
where
    miro_df.points != jira_df.points
""").show(max_rows=100, max_width=10000)
