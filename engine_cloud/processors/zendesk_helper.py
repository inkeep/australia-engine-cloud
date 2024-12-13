import os
import re
import requests
import json
import html
from toolz import itertoolz
from datetime import datetime, timedelta, timezone
from prefect import flow, task
import multiprocessing
from multiprocessing import Pool, Manager
import time
from concurrent.futures import ThreadPoolExecutor
from nango_helper import (
    get_nango_key,
    get_nango_connection_ids_by_type,
    get_zendesk_subdomain,
)
from helpers import get_content_hash

from mongo import (
    upload_batches,
    get_mongo_records_by_index_name,
    delete_mongodb_records_by_id,
)
from management_helpers import (
    create_source_sync_and_indexing_jobs,
    get_source,
)
from notion_helper import trigger_auto_index_deploy
from main import get_org_configs, create_index_mapping

THREE_YEARS_TIMESTAMP = datetime.now(timezone.utc) - timedelta(days=365 * 3)


def zendesk_api_helper(url, connection_id, response_key=None):
    api_key = get_nango_key("zendesk", connection_id)
    headers = {"Authorization": f"Bearer {api_key}"}
    base_url = url
    items = []
    while url:
        print(f"Getting {url}")
        response = requests.get(url, headers=headers)
        if int(response.headers.get("x-rate-limit-remaining", "0")) == 0:
            print(
                f"Rate limit remaining: {response.headers.get('x-rate-limit-remaining')}"
            )
            if response.headers.get("ratelimit-reset"):
                time.sleep(int(response.headers.get("ratelimit-reset")))
            else:
                time.sleep(60)
            response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if response_key:
                items.extend(data[response_key])
            else:
                items.append(data)
            if "meta" in data and data["meta"]["has_more"]:
                url = data.get("links").get("next")
            elif "next_page" in data:
                url = data.get("next_page")
            else:
                url = None
        else:
            print(url)
            print(f"Error: {response.status_code}, {response.text}")
            break
    return items


def get_zendesk_tickets_cursor(url, connection_id):
    api_key = get_nango_key("zendesk", connection_id)
    headers = {"Authorization": f"Bearer {api_key}"}
    tickets = []
    with requests.Session() as session:
        while url:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                tickets.extend(data["tickets"])

                if not data["end_of_stream"]:
                    url = data["after_url"]
                else:
                    print("End of stream reached. Saving cursor for next export.")
                    # Save the after_cursor for the next export
                    url = None
            else:
                print(f"Error: {response.status_code}, {response.text}")
                break

    return tickets


def get_zendesk_ticket_by_id(args):
    ticket_id, zendesk_subdomain, connection_id = args
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/tickets/{ticket_id}.json"
    ticket = zendesk_api_helper(url, connection_id, None)
    return ticket[0] if ticket else ticket_id


def get_zendesk_tickets(zendesk_subdomain, connection_id, start_time=None):
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=30)).timestamp())
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/incremental/tickets/cursor.json?start_time={start_time}&include=comment_count"
    print(f"Getting tickets from {url}")
    return get_zendesk_tickets_cursor(url, connection_id)


def get_zendesk_ticket_events(zendesk_subdomain, connection_id, start_time=None):
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=30)).timestamp())
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/incremental/ticket_events?start_time={start_time}&include=comment_events"
    print(f"Getting ticket events from {url}")
    return zendesk_api_helper(url, connection_id, "ticket_events")


def get_zendesk_articles(zendesk_subdomain, connection_id):
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/help_center/articles.json?page_size=100"
    articles = zendesk_api_helper(url, connection_id, "articles")
    return articles


def get_ticket_comments(ticket_id, zendesk_subdomain, connection_id):
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments.json"
    comments = zendesk_api_helper(url, connection_id, response_key="comments")
    return comments


def get_ticket_content(args):
    ticket, zendesk_subdomain, connection_id = args
    comments = get_ticket_comments(ticket["id"], zendesk_subdomain, connection_id)

    ticket_md = f"""
# {ticket['subject']}

Status: {ticket['status']}
Created: {ticket['created_at']}
Last Updated: {ticket['updated_at']}
    """

    for comment in comments:
        try:
            if comment["via"]["channel"] == "web":
                sender = "Customer via web"
            elif comment["via"]["channel"] == "api":
                sender = "API"
            elif "ticket_id" in comment.get("via", {}).get("source", {}).get(
                "from", {}
            ):
                sender = f'Follow-up on ticket #{comment["via"]["source"]["from"]["ticket_id"]} ({comment["via"]["source"]["from"]["subject"]})'
            elif comment.get("author", {}).get("name"):
                sender = f'{comment["author"]["name"]}'
            else:
                sender = "Unknown sender"

            # Determine receiver
            if comment["public"]:
                receiver = "Public"
            elif comment.get("via", {}).get("source", {}).get("to", {}).get("name"):
                receiver = f'{comment["via"]["source"]["to"]["name"]}'
            else:
                receiver = "Internal"

        except Exception as e:
            print(f"Error processing comment: {e}")
            sender = "Error determining sender"
            receiver = "Error determining receiver"
        comment_string = f"""
Message from {sender} to {receiver} on {comment['created_at']}:
    '''{html.unescape(comment['plain_body'])}'''

    """
        ticket_md += comment_string
    return ticket_md


def create_ticket_record(args):
    ticket, zendesk_subdomain, _ = args
    try:
        ticket_md = get_ticket_content(args)
    except Exception as e:
        print(f"Error creating ticket record: {e}")
        return None
    return {
        "record_id": get_content_hash(ticket_md),
        "content": ticket_md,
        "title": ticket["subject"],
        "url": f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket['id']}",
        "attributes": {
            "breadcrumbs": json.dumps(
                [
                    "Zendesk Support Tickets",
                    f"Ticket #{ticket['id']}",
                ]
            ),
        },
        "extra_attributes": {},
    }


def validate_ticket(ticket, time_delta=THREE_YEARS_TIMESTAMP):
    updated_at = datetime.fromisoformat(ticket["updated_at"].replace("Z", "+00:00"))
    return updated_at > time_delta and (
        ticket["status"] == "closed" or ticket["status"] == "solved"
    )


def get_updated_zendesk_records(
    connection_id,
    zendesk_subdomain,
    updated_tickets,
    mongo_ticket_id_map,
):
    deleted_ids = []
    overlapping_ticket_args = [
        (ticket, zendesk_subdomain, connection_id) for ticket in updated_tickets
    ]
    updated_tickets = get_ticket_records_parallel(
        overlapping_ticket_args, zendesk_subdomain, connection_id
    )

    output_tickets = []
    for i, current_ticket_record in enumerate(updated_tickets):
        id = int(current_ticket_record["url"].split("/")[-1])
        current_ticket_md = current_ticket_record["content"]
        if id in mongo_ticket_id_map and mongo_ticket_id_map[id][
            "record_id"
        ] != get_content_hash(current_ticket_md):
            output_tickets.append(current_ticket_record)
            deleted_ids.append(id)
        elif id not in mongo_ticket_id_map:
            output_tickets.append(current_ticket_record)

    return output_tickets, deleted_ids


def get_created_at_string(ticket_md):
    for line in ticket_md.split("\n"):
        if re.match(r"Created: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", line):
            return line

    return None


def get_records_past_three_months(mongo_ticket_id_map):
    valid_ticket_ids = set()
    for id, mongo_record in mongo_ticket_id_map.items():
        created_at_line = get_created_at_string(mongo_record["content"])
        if created_at_line:
            created_at = datetime.fromisoformat(
                created_at_line.split("Created: ")[1].replace("Z", "+00:00")
            )

        message_lines = []
        for line in mongo_record["content"].split("\n"):
            if "Message" in line.strip() and re.search(
                r"on \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z:", line.strip()
            ):
                message_lines.append(line[0:-1])
        if len(message_lines) > 0:
            created_at = datetime.fromisoformat(
                message_lines[-1].split("on ")[-1].replace("Z", "+00:00")
            )
        three_months_ago = datetime.now(timezone.utc) - timedelta(days=91)
        if created_at > three_months_ago:
            valid_ticket_ids.add(id)

    return valid_ticket_ids


def get_tickets_by_id(ids, zendesk_subdomain, connection_id):
    tickets = []
    args = [(id, zendesk_subdomain, connection_id) for id in ids]
    if args:
        with ThreadPoolExecutor(max_workers=min(32, len(args))) as executor:
            tickets = list(filter(None, executor.map(get_zendesk_ticket_by_id, args)))
    deleted_ticket_ids = set()
    found_tickets = []
    for ticket in tickets:
        if type(ticket) == int:
            deleted_ticket_ids.add(ticket)
        else:
            found_tickets.append(ticket["ticket"])

    return found_tickets, deleted_ticket_ids


def get_new_zendesk_tickets(zendesk_subdomain, connection_id, current_mongo_record_map):
    new_tickets = []
    past_two_days = datetime.now(timezone.utc) - timedelta(days=2)
    recent_zendesk_tickets = get_zendesk_tickets(
        zendesk_subdomain, connection_id, start_time=past_two_days.timestamp()
    )
    zendesk_ticket_id_map = {ticket["id"]: ticket for ticket in recent_zendesk_tickets}
    arg_list = []
    for ticket_id, ticket in zendesk_ticket_id_map.items():
        if ticket_id not in current_mongo_record_map and validate_ticket(ticket):
            arg_list.append((ticket, zendesk_subdomain, connection_id))

    new_tickets = get_ticket_records_parallel(
        arg_list, zendesk_subdomain, connection_id
    )

    return new_tickets


def get_ticket_metadata(zendesk_subdomain, connection_id, start_time=None):
    # url = (
    #     f"https://{zendesk_subdomain}.zendesk.com/api/v2/ticket_metrics?page[size]=100"
    # )
    base_url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/ticket_metrics"
    params = ["page[size]=100"]

    if start_time:
        # Convert to Unix timestamp if datetime object is provided
        if isinstance(start_time, datetime):
            start_time = int(start_time.timestamp())
        params.append(f"start_time={start_time}")

    url = f"{base_url}?{'&'.join(params)}"
    return zendesk_api_helper(url, connection_id, "ticket_metrics")


def get_ticket_events(zendesk_subdomain, connection_id):
    past_three_months = datetime.now(timezone.utc) - timedelta(days=91)
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/ticket_events.json?start_time={past_three_months.timestamp()}"
    return zendesk_api_helper(url, connection_id, "ticket_events")


def get_updated_ticket_ids(metadata_map, mongo_ticket_id_map):
    updated_ids = []
    for ticket_id in mongo_ticket_id_map:
        if int(ticket_id) in metadata_map:
            metadata_val = metadata_map[int(ticket_id)]
            updated_at = metadata_val["updated_at"]
            updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            two_days = datetime.now(timezone.utc) - timedelta(days=2)
            if updated_at > two_days:
                updated_ids.append(ticket_id)
    return updated_ids


def get_updated_tickets(ticket_id_map, mongo_ticket_id_map):
    updated_tickets = []
    for id, record in mongo_ticket_id_map.items():
        if id in ticket_id_map:
            comment_count = record["content"].count("Message ")
            if comment_count < ticket_id_map[id]["comment_count"]:
                updated_tickets.append(ticket_id_map[id])

    for id, ticket in ticket_id_map.items():
        if validate_ticket(ticket) and id not in mongo_ticket_id_map:
            updated_tickets.append(ticket)

    return updated_tickets


@task(name="update_zendesk_records", log_prints=True)
def update_zendesk_records(
    connection_id, zendesk_subdomain, index_name, source_id, org_alias, project_id
):
    print(f"Updating zendesk records for {index_name}")
    current_mongo_records = get_mongo_records_by_index_name(index_name)

    mongo_ticket_id_map = {
        int(record["url"].split("/")[-1]): record for record in current_mongo_records
    }

    print("Getting new tickets")
    new_ticket_records = get_new_zendesk_tickets(
        zendesk_subdomain, connection_id, mongo_ticket_id_map
    )
    print(f"Found {len(new_ticket_records)} new tickets")

    valid_ticket_ids = get_records_past_three_months(mongo_ticket_id_map)
    print(f"Found {len(valid_ticket_ids)} valid tickets")
    past_three_months = datetime.now(timezone.utc) - timedelta(days=91)

    tickets_past_three_months = get_zendesk_tickets(
        zendesk_subdomain, connection_id, start_time=past_three_months.timestamp()
    )
    ticket_id_map = {x["id"]: x for x in tickets_past_three_months}
    deleted_ids = set(valid_ticket_ids) - set(
        [x["id"] for x in tickets_past_three_months]
    )
    print(f"Found {len(deleted_ids)} deleted tickets")

    updated_tickets = get_updated_tickets(ticket_id_map, mongo_ticket_id_map)
    print(f"Found {len(updated_tickets)} updated tickets")

    print("Getting updated tickets")
    updated_records, outdated_ids = get_updated_zendesk_records(
        connection_id,
        zendesk_subdomain,
        updated_tickets,
        mongo_ticket_id_map,
    )
    print(f"Found {len(updated_records)} updated tickets")

    updated_records.extend(new_ticket_records)

    deleted_ids |= set(outdated_ids)

    print(f"Deleting {len(deleted_ids)} records")
    if len(deleted_ids) > 0:
        delete_mongodb_records_by_id(
            [mongo_ticket_id_map[id]["record_id"] for id in deleted_ids]
        )

    if len(updated_records) > 0:
        print(f"Uploading {len(updated_records)} new/updated records")
        upload_batches(updated_records, index_name, source_id, org_alias, project_id)

    return updated_records, deleted_ids


def get_rate_limit_reset(zendesk_subdomain, connection_id):
    url = f"https://{zendesk_subdomain}.zendesk.com/api/v2/tickets/1/comments.json"
    api_key = get_nango_key("zendesk", connection_id)
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    return response.headers.get("ratelimit-reset")


def get_ticket_records_parallel(arg_list, zendesk_subdomain, connection_id):
    if not arg_list:  # Return empty list if no arguments provided
        return []
    records = []
    chunks = itertoolz.partition_all(300, arg_list)
    for chunk in chunks:
        with ThreadPoolExecutor(max_workers=min(32, len(chunk))) as executor:
            chunk_records = list(
                filter(None, executor.map(create_ticket_record, chunk))
            )
            records.extend(chunk_records)
        reset_time = get_rate_limit_reset(zendesk_subdomain, connection_id)
        print(f"Rate limit reset time: {reset_time}")
        time.sleep(int(reset_time))

    return records


def get_ticket_records_serial(arg_list):
    records = []
    for args in arg_list:
        records.append(create_ticket_record(args))
    return records


@task(name="upload_zendesk_records", log_prints=True)
def upload_zendesk_records_task(
    org_alias,
    index_name,
    source_id,
    project_id,
    connection_id,
    zendesk_subdomain,
    source_namespace,
    start_time=None,
):
    records = []
    tickets = get_zendesk_tickets(zendesk_subdomain, connection_id, start_time)
    print(f"Found {len(tickets)} tickets")
    valid_tickets = [ticket for ticket in tickets if validate_ticket(ticket)]
    print(f"Found {len(valid_tickets)} valid tickets")
    args = [(ticket, zendesk_subdomain, connection_id) for ticket in valid_tickets]

    records = get_ticket_records_parallel(args, zendesk_subdomain, connection_id)
    filtered_records = []
    for record in records:
        if record:
            filtered_records.append(record)
    upload_batches(filtered_records, index_name, source_id, org_alias, project_id)

    create_source_sync_and_indexing_jobs(
        org_alias,
        source_id,
        status="PROCESSING",
        status_message="ingested",
    )
    trigger_auto_index_deploy(
        org_alias,
        {source_id: {source_namespace: [index_name]}},
    )


@flow(name="run_initial_zendesk_upload_flow", log_prints=True)
def run_initial_zendesk_upload_flow(org, connection_id):
    configs = get_org_configs(org)
    for config in configs.get("zendesk_tickets", []):
        if config["input"].nango_info.connection_id == connection_id:
            zendesk_subdomain = get_zendesk_subdomain(connection_id)
            start_time = (datetime.now() - timedelta(days=30)).timestamp()
            upload_zendesk_records_task(
                org,
                config["base_index_name"],
                config["input"].source_id,
                config["input"].project_ids[0],
                connection_id,
                zendesk_subdomain,
                config["input"].source_id,
                start_time,
            )


@flow(name="upload_zendesk_tickets_flow", log_prints=True)
def upload_zendesk_tickets_flow():
    connection_ids = get_nango_connection_ids_by_type("zendesk")
    for connection_id in connection_ids:
        zendesk_subdomain = get_zendesk_subdomain(connection_id)
        print(f"Uploading zendesk tickets for {zendesk_subdomain}")
        org = connection_id.split(":")[0]
        configs = get_org_configs(org)
        configs = configs.get("zendesk_tickets", [])
        print(f"Found {len(configs)} zendesk configs")
        for config in configs:
            config_input = config["input"]
            if config["input"].nango_info.connection_id == connection_id:
                source = get_source(config_input.source_id, org)
                if source["indexes"]:
                    upload_zendesk_records_task(
                        org,
                        config["base_index_name"],
                        config_input.source_id,
                        config_input.project_ids[0],
                        connection_id,
                        zendesk_subdomain,
                        source["indexes"][0]["id"],
                    )


@flow(name="update_zendesk_tickets_flow", log_prints=True)
def update_zendesk_flow():
    connection_ids = get_nango_connection_ids_by_type("zendesk")
    for connection_id in connection_ids:
        org = connection_id.split(":")[0]
        configs = get_org_configs(org)
        mapping = create_index_mapping(org, configs)
        zendesk_configs = configs.get("zendesk_tickets", [])
        zendesk_subdomain = get_zendesk_subdomain(connection_id)
        for config in zendesk_configs:
            if get_mongo_records_by_index_name(config["base_index_name"]):
                update_zendesk_records(
                    connection_id,
                    zendesk_subdomain,
                    config["base_index_name"],
                    config["input"].source_id,
                    org,
                    config["input"].project_ids[0],
                )
                create_source_sync_and_indexing_jobs(
                    org,
                    config["input"].source_id,
                    status="PROCESSING",
                    status_message="ingested",
                )
                trigger_auto_index_deploy(
                    org,
                    {config["input"].source_id: mapping[config["input"].source_id]},
                    deployment_name="index_sync_job_flow/gcp-zendesk-indexing-deploy",
                )

    # if __name__ == "__main__":
    #     connection_id = "midjourney:zendesk:ADe4fqyVOz26FaSJo09Dz"
    #     zendesk_subdomain = get_zendesk_subdomain(connection_id)
    #     configs = get_org_configs("midjourney")
    #     print(configs.keys())


#     config = configs["zendesk_tickets"][0]
#     print(config["input"].project_ids[0])
#     update_zendesk_records(
#         connection_id,
#         zendesk_subdomain,
#         config["base_index_name"],
#         config["input"].source_id,
#         "midjourney",
#         config["input"].project_ids[0],
#     )


# #     print(f"Updating zendesk records for {config['base_index_name']}")
# #     current_mongo_records = get_mongo_records_by_index_name(config["base_index_name"])
# #     mongo_ticket_id_map = {
# #         int(record["url"].split("/")[-1]): record for record in current_mongo_records
# #     }
# #     valid_ticket_ids = get_records_past_three_months(mongo_ticket_id_map)
# #     print(f"Found {len(valid_ticket_ids)} valid tickets")

# #     print(len(valid_ticket_ids))
# #     all_metadata = []
# #     metadata = get_ticket_metadata(zendesk_subdomain, connection_id)

# #     tickets, deleted_ids = get_tickets_by_id(
# #         ["97970"], zendesk_subdomain, connection_id
# #     )
# #     print(tickets)
# #     print(deleted_ids)
# #     # print(validate_ticket(tickets[0]))
