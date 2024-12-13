import json
import requests
from datetime import datetime, timezone
from prefect import flow, task
from requests.auth import HTTPBasicAuth
from config import settings
from mongo import (
    upload_batches,
    delete_mongodb_records_by_index_name,
)
from md_helper import convert_html_to_md
from helpers import get_content_hash
from management_helpers import (
    create_source_sync_and_indexing_jobs,
)
from notion_helper import trigger_auto_index_deploy


def get_all_pages(pages_url, base_url):
    auth = HTTPBasicAuth("miles@inkeep.com", settings.INTELYCARE_CONFLUENCE_TOKEN)
    headers = {"Accept": "application/json"}
    query_params = {"body-format": "storage", "limit": "100"}
    pages = []
    response = requests.get(pages_url, headers=headers, params=query_params, auth=auth)
    body = json.loads(response.text)
    next_link = body["_links"]
    next = next_link.get("next", None)
    print(f"Found {len(body['results'])} pages")
    pages.extend(body["results"])
    while next:
        response = requests.get(base_url + next_link["next"], headers=headers)
        body = json.loads(response.text)
        next_link = body.get("_links", None)
        next = next_link.get("next", None)
        results = body.get("results", [])
        print(f"found {len(results)} pages")
        pages.extend(results)
    return pages


def filter_old_pages(pages):
    comparison_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    filtered_pages = []
    for page in pages:
        date_obj = datetime.fromisoformat(page["createdAt"].replace("Z", "+00:00"))
        if date_obj >= comparison_date:
            filtered_pages.append(page)
    return filtered_pages


@task(name="upload-records-task")
def upload_confluence_records(
    index_name, source_id, org_alias, project_id, base_url, pages_url, filter_old=True
):
    pages = get_all_pages(pages_url, base_url)
    page_id_map = {page["id"]: page for page in pages}
    if filter_old:
        pages = filter_old_pages(pages)
    records = []
    for page in pages:
        content = convert_html_to_md(page["body"]["storage"]["value"])
        records.append(
            {
                "record_id": get_content_hash(content),
                "content": content,
                "url": base_url + "/wiki" + page["_links"]["webui"],
                "title": page["title"],
                "attributes": {
                    "authorId": page["authorId"] if page["authorId"] else "",
                    "createdAt": page["createdAt"] if page["createdAt"] else "",
                    "pageId": page["id"] if page["id"] else "",
                    "breadcrumbs": json.dumps(get_breadcrumbs(page_id_map, page)),
                },
                "extra_attributes": {},
            }
        )
    delete_mongodb_records_by_index_name(index_name)
    upload_batches(records, index_name, source_id, org_alias, project_id)


@task(name="upload_confluence_records", log_prints=True)
def upload_confluence_records_task(
    index_name, source_id, org_alias, project_id, base_url, pages_url, filter_old=True
):
    upload_confluence_records(
        index_name, source_id, org_alias, project_id, base_url, pages_url, filter_old
    )


def get_breadcrumbs(page_id_map, page):
    curr_page = page
    breadcrumbs = [page["title"]]
    while curr_page["parentId"]:
        parent_page = page_id_map[curr_page["parentId"]]
        breadcrumbs.append(parent_page["title"])
        curr_page = parent_page
    return breadcrumbs[::-1]


@flow(name="sync_confluence_flow", log_prints=True)
def update_confluence_flow():
    base_url = "https://hivemq.atlassian.net"
    pages_url = "https://hivemq.atlassian.net/wiki/api/v2/pages"
    print("updating hivemq confluence")
    upload_confluence_records.with_options(name=f"hivemq-confluence-task")(
        "inkeep_processed_hivemq_docs-hivemq-atlassian-net_graph",
        "clz0cv6lh004p7qs26baa6rvv",
        "hivemq",
        "clz0cv6lh004p7qs26baa6rvv",
        base_url,
        pages_url,
    )
    create_source_sync_and_indexing_jobs(
        "hivemq",
        "clz0cv6lh004p7qs26baa6rvv",
        status="PROCESSING",
        status_message="ingested",
    )
    trigger_auto_index_deploy(
        "hivemq",
        {
            "clz0cv6lh004p7qs26baa6rvv": {
                "hivemq_docs-hivemq-atlassian-net_20240724": [
                    "inkeep_processed_hivemq_docs-hivemq-atlassian-net_graph"
                ]
            }
        },
    )
    print("Updating deliverlogic Confluence")
    upload_confluence_records.with_options(name=f"deliverlogic-confluence-task")(
        "inkeep_processed_deliverlogic_site-deliverlogic-atlassian-net-wiki-spaces-BSD-overview_graph",
        "cm0zovmeg0014ftw3cvhuicmb",
        "deliverlogic",
        "cm0zo9cs30012ftw3gaxckglg",
        "https://deliverlogic.atlassian.net",
        "https://deliverlogic.atlassian.net/wiki/api/v2/pages",
    )
    create_source_sync_and_indexing_jobs(
        "deliverlogic",
        "cm0zovmeg0014ftw3cvhuicmb",
        status="PROCESSING",
        status_message="ingested",
    )
    trigger_auto_index_deploy(
        "deliverlogic",
        {
            "cm0zovmeg0014ftw3cvhuicmb": {
                "deliverlogic_site-deliverlogic-atlassian-net-wiki-spaces-BSD-overview_20240912": [
                    "inkeep_processed_deliverlogic_site-deliverlogic-atlassian-net-wiki-spaces-BSD-overview_graph"
                ]
            }
        },
    )


# if __name__ == "__main__":
#     upload_confluence_records(
#         "inkeep_processed_deliverlogic_site-deliverlogic-atlassian-net-wiki-spaces-BSD-overview_graph",
#         "cm0zovmeg0014ftw3cvhuicmb",
#         "deliverlogic",
#         "cm0zo9cs30012ftw3gaxckglg",
#         "https://deliverlogic.atlassian.net",
#         "https://deliverlogic.atlassian.net/wiki/api/v2/pages",
#     )
