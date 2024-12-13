from supabase import create_client, Client
import json
from prefect import flow, task

from config import settings
from mongo import (
    upload_batches,
    delete_mongodb_records_by_index_name,
)
from helpers import get_content_hash, get_num_tokens, get_org_configs
from main import trigger_auto_index_deploy
from management_helpers import create_source_sync_and_indexing_jobs


def get_page_count(client, org_id, table_name):
    total_count_response = (
        client.table(table_name)
        .select("id", count="exact")  # Use an appropriate column for counting
        .eq("organization_id", org_id)
        .execute()
    )

    page_size = 1000
    total_count = total_count_response.count
    total_pages = (total_count + page_size - 1) // page_size
    return total_pages


def get_records(client, org_id: str, table: str, column_name: str, column_val: str):
    page_size = 1000
    results = []
    total_pages = get_page_count(client, org_id, table)
    for page_number in range(total_pages):
        results.extend(
            client.table(table)
            .select("*")
            .eq(column_name, column_val)
            .range(page_number * page_size, (page_number + 1) * page_size - 1)
            .execute()
            .data
        )
    return results


def get_analytics_conversations(org_id: str):
    client: Client = create_client(
        settings.SUPABASE_ANALYTICS_URL, settings.SUPABASE_ANALYTICS_KEY
    )
    conversations = get_records(
        client, org_id, "conversations", "organization_id", org_id
    )
    messages = get_records(client, org_id, "messages", "organization_id", org_id)
    convo_map = {}
    for conversation in conversations:
        convo_map[conversation["id"]] = conversation
        conversation["messages"] = []

    for message in messages:
        if message["conversation_id"] in convo_map:
            convo_map[message["conversation_id"]]["messages"].append(message)

    return convo_map


def construct_conversation(messages):
    md = ""
    if messages:
        md += f"# Clerk {messages[0]['name']}\n\n"
    for message in messages:
        md += f"Message:\n {message['content']}\n\n"
    return md


def process_conversation(conversation):
    if not conversation["messages"]:
        return None
    messages = conversation["messages"]
    # Sort messages by created_at timestamp
    sorted_messages = sorted(messages, key=lambda x: x["order"])
    messages = sorted_messages
    url = conversation["external_url"]
    content = construct_conversation(messages)
    if messages[0]["name"] == "Plain Thread":
        title = "Clerk Plain Ticket"
        breadcrumbs = ["Clerk", "Plain Tickets"]
    else:
        title = "Clerk Slack Thread"
        breadcrumbs = ["Clerk", "Slack Threads"]
    return {
        "record_id": get_content_hash(content),
        "format": "md",
        "title": title,
        "url": url,
        "content": content,
        "attributes": {
            "hash_content": get_content_hash(content),
            "num_tokens": get_num_tokens(content),
            "breadcrumbs": json.dumps(breadcrumbs),
        },
        "extra_attributes": {},
    }


@task(name="get_conversation_records", log_prints=True)
def get_conversation_records(org_id):
    conversations = get_analytics_conversations(org_id)
    slack_records = []
    plain_records = []
    print(f"found {len(conversations)} conversations")
    for convo in conversations.values():
        record = process_conversation(convo)
        if "Slack" in record["title"]:
            print(f"found slack record: {record['title']}")
            slack_records.append(record)
        else:
            print(f"found plain record: {record['title']}")
            plain_records.append(record)

    delete_mongodb_records_by_index_name("inkeep_processed_clerk_plain_graph")
    print(f"uploading {len(plain_records)} plain records")
    upload_batches(
        plain_records,
        "inkeep_processed_clerk_plain_graph",
        "cm2je2k36040nuk7kddsc1kei",
        "clerk",
        "cm074llyj0013uzbwvqkkrvsq",
    )
    print(f"uploading {len(slack_records)} slack records")
    delete_mongodb_records_by_index_name("inkeep_processed_clerk_slack-threads_graph")
    upload_batches(
        slack_records,
        "inkeep_processed_clerk_slack-threads_graph",
        "cm3d6pvyz00161dl7dx0bmepu",
        "clerk",
        "cm074llyj0013uzbwvqkkrvsq",
    )


@flow(name="run_conversation_update_flow", log_prints=True)
def run_conversation_update_flow():
    records = get_conversation_records("org_zj86aOQ7cUFHnZ9S")
    print(f"found {len(records)} records")
    configs = get_org_configs("clerk", map_source_ids=True)
    create_source_sync_and_indexing_jobs(
        "clerk",
        "cm2je2k36040nuk7kddsc1kei",
        status="PROCESSING",
        status_message="ingested",
    )
    create_source_sync_and_indexing_jobs(
        "clerk",
        "cm3d6pvyz00161dl7dx0bmepu",
        status="PROCESSING",
        status_message="ingested",
    )
    trigger_auto_index_deploy("clerk", configs["cm2je2k36040nuk7kddsc1kei"])
    trigger_auto_index_deploy("clerk", configs["cm3d6pvyz00161dl7dx0bmepu"])
