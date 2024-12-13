import json
import requests
import backoff
from prefect import task, flow
from prefect.deployments import run_deployment
from config import settings
from helpers import get_content_hash, get_url_from_md
from management_helpers import (
    create_source_sync_and_indexing_jobs,
    get_source,
)
from mongo import (
    delete_mongodb_records_by_metadata,
    MongoRecordMetadataQueryInput,
    upload_batches,
)
from nango_helper import get_nango_connection_ids_by_type, get_nango_key
from helpers import get_org_configs
from main import create_index_mapping, ping_source_sync_indexer


# def get_notion_source(org_alias, connection_id):


@task(name="trigger-indexing-task")
def trigger_auto_index_deploy(org, mapping, deployment_name="index_sync_job_flow/gcp-auto-indexing-deploy"):
    body = {"index_mapping": mapping, "org_alias": org}
    deployment_run = run_deployment(
        name=deployment_name, parameters={"input": body}
    )
    return deployment_run
    # ping_source_sync_indexer(org, )


import time


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
def make_api_request(url, headers, json=None, params=None, get_json=True):
    while True:
        if json is not None:
            response = requests.post(url, headers=headers, json=json)
        else:
            response = requests.get(url, headers=headers, params=params)
        print(response.text)

        if response.status_code == 429:  # Rate limit error
            reset_time = int(response.headers.get("Retry-After", time.time() + 120))
            sleep_time = max(reset_time - time.time(), 1)
            print(f"Rate limited. Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
            continue

        response.raise_for_status()

        if get_json:
            return response.json()
        else:
            return response


def get_all_pages(token=settings.BASETEN_NOTION_TOKEN):
    url = "https://api.notion.com/v1/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    json_body = {"filter": {"property": "object", "value": "page"}}
    response = make_api_request(url, headers, json=json_body)

    all_pages = []
    start_cursor = None

    while True:
        if start_cursor:
            json_body["start_cursor"] = start_cursor
        response = make_api_request(url, headers, json=json_body)
        data = response
        all_pages.extend(data.get("results", []))
        if not data.get("has_more", False):
            break
        start_cursor = data.get("next_cursor", None)
    response_json = response

    return response_json


def get_blocks_for_page(
    page_id, start_cursor=None, token=settings.BASETEN_NOTION_TOKEN
):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    params = {}
    if start_cursor:
        params["start_cursor"] = start_cursor
    response = make_api_request(url, headers, params=params)

    return response


def get_all_blocks(page_id, token=settings.BASETEN_NOTION_TOKEN):
    all_blocks = []
    start_cursor = None
    while True:
        response = get_blocks_for_page(page_id, start_cursor, token=token)
        if response:
            all_blocks.extend(response.get("results", []))
            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")
        else:
            break
    return all_blocks


def convert_blocks_to_md(blocks, token):
    md = ""
    for block in blocks:
        if block["type"] == "paragraph":
            md += convert_rich_text_to_md(block["paragraph"]["rich_text"]) + "\n\n"
        elif block["type"] == "heading_1":
            md += f"# {convert_rich_text_to_md(block['heading_1']['rich_text'])}\n\n"
        elif block["type"] == "heading_2":
            md += f"## {convert_rich_text_to_md(block['heading_2']['rich_text'])}\n\n"
        elif block["type"] == "heading_3":
            md += f"### {convert_rich_text_to_md(block['heading_3']['rich_text'])}\n\n"
        elif block["type"] == "bulleted_list_item":
            md += f"- {convert_rich_text_to_md(block['bulleted_list_item']['rich_text'])}\n"
        elif block["type"] == "numbered_list_item":
            md += f"1. {convert_rich_text_to_md(block['numbered_list_item']['rich_text'])}\n"
        elif block["type"] == "to_do":
            checked = "x" if block["to_do"]["checked"] else " "
            md += f"- [{checked}] {convert_rich_text_to_md(block['to_do']['rich_text'])}\n"
        elif block["type"] == "code":
            language = block["code"]["language"]
            code = convert_rich_text_to_md(block["code"]["rich_text"])
            md += f"```{language}\n{code}\n```\n\n"
        elif block["type"] == "quote":
            md += f"> {convert_rich_text_to_md(block['quote']['rich_text'])}\n\n"
        elif block["type"] == "divider":
            md += "---\n\n"
        elif block["type"] == "image":
            md += handle_image_block(block)
        elif block["type"] == "table":
            md += handle_table_block(block, token)
        # Add more block types as needed

        if block.get("has_children", False):
            child_blocks = get_block_children(block["id"], token)
            md += convert_blocks_to_md(child_blocks, token)

    return md


def parse_table_rows(rows):
    table_data = []
    for row in rows:
        if row["type"] == "table_row":
            cells = [
                cell[0]["plain_text"] for cell in row["table_row"]["cells"] if cell
            ]
            table_data.append(cells)
    return table_data


def format_markdown_table(data):
    if not data:
        return "No data available"

    # Create the header row
    headers = data[0]
    markdown = "| " + " | ".join(headers) + " |\n"

    # Create the separator
    separator = "| " + " | ".join(["---"] * len(headers)) + " |\n"

    # Add rows
    markdown += separator
    for row in data[1:]:
        markdown += "| " + " | ".join(row) + " |\n"

    return markdown


def get_page(page_id, token=settings.BASETEN_NOTION_TOKEN):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    response = make_api_request(url, headers)
    return response


def get_block_children(block_id, token):
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    response = make_api_request(url, headers)
    try:
        response_json = response
    except Exception as e:
        print(response)
        print(response.text)
        raise (e)
    return response_json["results"]


def convert_columns_to_markdown(columns, token):
    markdown_content = "<table><tr>"  # Using HTML table for column layout in Markdown
    for column in columns:
        markdown_content += "<td>"
        block_children = get_block_children(column["id"], token)
        markdown_content += convert_blocks_to_md(block_children, token)
        markdown_content += "</td>"
    markdown_content += "</tr></table>"
    return markdown_content


def get_all_child_page_md(pages, token=settings.BASETEN_NOTION_TOKEN):
    records = []
    for i, page in enumerate(pages):
        print(f"Processing page {i+1}/{len(pages)}")
        properties = page["properties"]
        page_id = page["id"]
        blocks = get_all_blocks(page_id, token=token)
        print(f"Found {len(blocks)} blocks")

        md, url = convert_properties_to_md(properties)
        md += convert_blocks_to_md(blocks, token)

        title = extract_title(properties)
        if title:
            md = f"# {title}\n\n" + md
        if page["url"] == "www.notion.so/56f833e14ce7443e94f96ee044e408a4":
            continue
        records.append(
            {
                "record_id": get_content_hash(md),
                "content": md,
                "title": title,
                "url": url if url else page["url"],
                "attributes": extract_attributes(properties),
                "extra_attributes": {},
            }
        )

        # Process child pages
        child_pages = [block for block in blocks if block["type"] == "child_page"]
        if child_pages:
            child_page_data = [
                get_page(block["id"], token=token) for block in child_pages
            ]
            records.extend(get_all_child_page_md(child_page_data, token=token))

    return records


def convert_properties_to_md(properties):
    md = ""
    url = None
    for key, value in properties.items():
        if key.lower() != "title":  # Skip title as it's handled separately
            md += f"**{key}**: {extract_property_value(value)}\n"
        if key == "Related URLs":
            url = get_url_from_md(extract_property_value(value))
    md = md + "\n" if md else ""
    return md, url


def extract_property_value(property):
    prop_type = property["type"]
    if prop_type == "rich_text":
        return convert_rich_text_to_md(property["rich_text"])
    elif prop_type == "select":
        return property["select"]["name"] if property["select"] else ""
    elif prop_type == "multi_select":
        return ", ".join([option["name"] for option in property["multi_select"]])
    elif prop_type == "date":
        return format_date(property["date"])
    elif prop_type == "checkbox":
        return "Yes" if property["checkbox"] else "No"
    # Add more property types as needed
    return ""


def extract_title(properties):
    title_keys = ["title", "Name", "Title", "Question from EA", "Question/Issue"]
    for key in title_keys:
        if key in properties and properties[key]["title"]:
            return convert_rich_text_to_md(properties[key]["title"])
    return ""


def handle_image_block(block):
    caption = convert_rich_text_to_md(block["image"].get("caption", []))
    if block["image"]["type"] == "external":
        url = block["image"]["external"]["url"]
    else:
        url = block["image"]["file"]["url"]
    return f"![{caption}]({url})\n\n"


def handle_table_block(block, token):
    rows = get_block_children(block["id"], token)
    table_data = parse_table_rows(rows)
    return format_markdown_table(table_data) + "\n\n"


def convert_rich_text_to_md(rich_text):
    md = ""
    for text in rich_text:
        content = text["plain_text"]
        annotations = text["annotations"]
        if annotations["bold"]:
            content = f"**{content}**"
        if annotations["italic"]:
            content = f"*{content}*"
        if annotations["strikethrough"]:
            content = f"~~{content}~~"
        if annotations["underline"]:
            content = f"<u>{content}</u>"
        if annotations["code"]:
            content = f"`{content}`"
        if text.get("href"):
            content = f"[{content}]({text['href']})"
        md += content
    return md


def extract_attributes(properties):
    attributes = {}
    for key, value in properties.items():
        if key.lower() != "title":
            attributes[key] = extract_property_value(value)
    return attributes


def format_date(date_obj):
    if not date_obj:
        return ""
    start = date_obj.get("start", "")
    end = date_obj.get("end", "")
    if end:
        return f"{start} to {end}"
    return start


def get_all_page_md(all_pages=[], token=settings.BASETEN_NOTION_TOKEN):
    records = []
    # types = set()
    if not all_pages:
        all_pages = get_all_pages(token=token)["results"]
    records = get_all_child_page_md(all_pages, token=token)
    # return types
    return records


def get_all_blocks_of_type(block_type, blocks, token=settings.BASETEN_NOTION_TOKEN):
    all_pages = get_all_pages(token=token)
    tables = []
    for page_id, blocks in blocks.items():
        for block in blocks:
            if block["type"] == block_type:
                tables.append(block)
    return tables


@task(name="get-page-md-task", log_prints=True)
def get_all_page_md_task(token=settings.BASETEN_NOTION_TOKEN):
    return get_all_page_md(token=token)


def get_all_blocks_of_type(block_type, blocks, token=settings.BASETEN_NOTION_TOKEN):
    all_pages = get_all_pages(token=token)
    tables = []
    for page_id, blocks in blocks.items():
        for block in blocks:
            if block["type"] == block_type:
                tables.append(block)
    return tables


def search_databases(integration_token):
    url = "https://api.notion.com/v1/search"
    headers = {
        "Authorization": f"Bearer {integration_token}",
        "Notion-Version": "2022-06-28",  # Always use the latest supported version
        "Content-Type": "application/json",
    }
    data = {
        "filter": {"value": "database", "property": "object"},
        "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
    }
    response = make_api_request(url, headers, json=data, get_json=False)
    if response.status_code == 200:
        databases = response.json().get("results", [])
        return databases
    else:
        print(f"Failed to search databases: {response.status_code}")
        return None


def query_database(database_id, integration_token):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {integration_token}",
        "Notion-Version": "2022-06-28",  # Use the latest API version
        "Content-Type": "application/json",
    }
    has_more = True
    next_cursor = None
    all_pages = []

    while has_more:
        data = {}
        if next_cursor:
            data["start_cursor"] = next_cursor

        response = make_api_request(url, headers, json=data, get_json=False)
        print(response.status_code)
        if response.status_code == 200:
            response_json = response.json()
            all_pages.extend(response_json["results"])
            has_more = response_json["has_more"]
            next_cursor = response_json.get("next_cursor")
        else:
            print(f"Failed to query database: {response.status_code}")
            return None

    return all_pages


def get_all_dbs(token):
    db_records = []
    dbs = search_databases(token)
    for db in dbs:
        print(db["id"])
        pages = query_database(db["id"], token)
        db_records.extend(get_all_child_page_md(pages, token))
    return db_records


def get_database(id, start_cursor=None, token=settings.BASETEN_NOTION_TOKEN):
    url = f"https://api.notion.com/v1/databases/{id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    params = {}
    if start_cursor:
        params["start_cursor"] = start_cursor
    response = make_api_request(url, headers, params=params)
    response_json = response.json()
    return response_json


def get_databases(token=settings.BASETEN_NOTION_TOKEN):
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    url = "https://api.notion.com/v1/search"
    payload = {"filter": {"property": "object", "value": "database"}}
    response = make_api_request(url, headers, json=payload)
    results = response.json()["results"]

    # Handling pagination
    while "next_cursor" in response.json() and response.json()["next_cursor"]:
        payload["start_cursor"] = response.json()["next_cursor"]
        response = make_api_request(url, headers, json=payload)
        results.extend(response.json()["results"])
    return results


@task(log_prints=True, name="upload-notion-records-task")
def upload_notion_records_task(
    index_name,
    token,
    source_id,
    project_id,
    org_name,
    source_namespace,
    run_delete=True,
    ping_ingester=True,
):
    upload_notion_records(
        index_name,
        token,
        source_id,
        project_id,
        org_name,
        source_namespace,
        run_delete=run_delete,
        ping_ingester=ping_ingester,
    )


def upload_notion_records(
    index_name,
    token,
    source_id,
    project_id,
    org_name,
    source_namespace,
    run_delete=True,
    ping_ingester=True,
):
    if run_delete:
        delete_mongodb_records_by_metadata(
            MongoRecordMetadataQueryInput(index_name=index_name)
        )
    records = get_all_page_md(token=token)
    records.extend(get_all_dbs(token))
    filtered_records = []
    seen = set()
    for record in records:
        if record["record_id"] in seen:
            continue
        else:
            filtered_records.append(record)
            seen.add(record["record_id"])
    upload_batches(records, index_name, source_id, org_name, project_id)
    if ping_ingester:
        create_source_sync_and_indexing_jobs(
            org_name, source_id, status="PROCESSING", status_message="ingested"
        )
        trigger_auto_index_deploy(
            org_name,
            {source_id: {source_namespace: [index_name]}},
        )


SKIP_IDS = ["clerk:notion", "bangert:notion"]


@task(log_prints=True)
def run_notion_pipeline_by_connection_id(connection_id):
    connection_id_split = connection_id.split(":")
    if len(connection_id_split) != 3:
        return
    org = connection_id_split[0]
    if org == "neon":
        return
    token = get_nango_key("notion", connection_id)
    configs = get_org_configs(org)
    notion_configs = configs.get("notion", [])
    for config in notion_configs:
        config_input = config["input"]
        if config["input"].nango_info.connection_id == connection_id:
            source = get_source(config_input.source_id, org)
            if source["indexes"]:
                upload_notion_records(
                    config["base_index_name"],
                    token,
                    config_input.source_id,
                    config_input.project_ids[0],
                    org,
                    source["indexes"][0]["id"],
                )


@flow(name="sync-notion-flow", log_prints=True)
def run_full_notion_sync_flow():
    print("RUNNING XANO UPLOAD")
    upload_notion_records_task(
        "inkeep_xano_notion",
        settings.XANO_NOTION_TOKEN,
        "clw0tw5dh00408rj7q5tfu5pf",
        "clx1x4iiu000jyx3gdxumwe4k",
        "xano",
        "xano_notion_20240605",
    )
    upload_notion_records_task(
        "inkeep_clerk_notion",
        settings.CLERK_NOTION_TOKEN,
        "cm23niz9r00ifdcka9ffb4bqb",
        "cm074llyj0013uzbwvqkkrvsq",
        "clerk",
        "clerk_notion-private_20241010",
    )
    upload_notion_records_task(
        "inkeep_bangert_notion",
        settings.BANGERT_NOTION_TOKEN,
        "cm257jhvk00ceu50iy0lxxf46",
        "cm20x9cno00m7x0eqvxsgh8p1",
        "bangert",
        "bangert_notion_20241011",
    )
    connection_ids = get_nango_connection_ids_by_type("notion")
    for connection_id in connection_ids:
        print(f"Running for {connection_id}")
        run_notion_pipeline_by_connection_id(connection_id)


@flow(name="sync-notion-for-connection-flow", log_prints=True)
def run_notion_sync_for_connection_flow(connection_id):
    run_notion_pipeline_by_connection_id(connection_id)


# if __name__ == "__main__":
#     upload_notion_records_task(
#         "inkeep_clerk_notion",
#         settings.CLERK_NOTION_TOKEN,
#         "cm23niz9r00ifdcka9ffb4bqb",
#         "cm074llyj0013uzbwvqkkrvsq",
#         "clerk",
#         "clerk_notion-private_20241010",
#     )
