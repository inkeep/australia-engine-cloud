import hashlib
import json
import typing as T
import re
import time
from collections import defaultdict
from datetime import datetime
from prefect import flow, task
from config import settings
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from mongo import upload_batches, delete_mongodb_records_by_index_name
from helpers import get_org_configs
from main import create_index_mapping
from notion_helper import trigger_auto_index_deploy
from management_helpers import create_source_sync_and_indexing_jobs

SLACK_TOKEN_MAPPING = {
    "devmarketing": settings.SLACK_DEVMARKETING_KEY,
    "skyflow": settings.SLACK_SKYFLOW_KEY,
    "inkeepdev": settings.SLACK_INKEEP_KEY,
    "tossim": settings.SLACK_TOSS_INTERNAL_KEY,
    "deliverlogic": settings.SLACK_DELIVERLOGIC_KEY,
    "deephavenio": settings.SLACK_DEEPHAVEN_KEY,
    "deephavenio_internal": settings.SLACK_DEEPHAVEN_INTERNAL_KEY,
}


def format_thread_list(thread_list):
    """
    Format a list of slack message threads into a string for LLM processing.
    """
    thread_list = thread_list[::-1]
    formatted_conversation = ""
    for thread in thread_list:
        formatted_conversation += (
            format_thread(thread, 0) + "------------------------------------\n\n"
        )
    return formatted_conversation


def format_thread(thread, level):
    """
    Format a slack message thread into a string.
    """
    root = thread[0]
    formatted_message = format_message(root, level)

    # Format the replies
    if len(thread) > 1:
        formatted_message += "Replies:\n"
        for child in thread[1:]:
            print(child)
            formatted_message += format_message(child, 1)

    return formatted_message


def format_message(message, level):
    """
    Format a slack message into a string.
    Level indicates the indentation level of the string
    """
    content = message["text"]
    author = message["user"]["name"]
    timestamp = datetime.utcfromtimestamp(float(message["timestamp"])).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    prefix = "  " * level
    return f'{prefix}"""{content}""" from @{author} on {timestamp}\n'


def get_content_hash(content):
    raw_hash = hashlib.sha1(
        json.dumps({"html": content}, sort_keys=True).encode("utf-8")
    )
    return raw_hash.hexdigest()


def get_slack_client(slack_token):
    # initialize the client with your bot token
    client = WebClient(token=slack_token)
    return client


def get_channel_name(channel_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the conversations.info method using the WebClient
        result = client.conversations_info(channel=channel_id)

        return result["channel"]["name"]

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_team_id(channel_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the conversations.info method using the WebClient
        result = client.conversations_info(channel=channel_id)

        return result["channel"]["context_team_id"]

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_channels(slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the conversations.info method using the WebClient
        result = client.conversations_list()

        return result["channels"]

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def handle_rate_limits(func):
    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    # Get retry_after from headers or default to 1 second
                    delay = int(e.response.headers.get("Retry-After", 1))
                    print(f"Rate limited. Waiting {delay} seconds...")
                    time.sleep(delay)
                    continue
                raise e

    return wrapper


def get_channel_threads(channel_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    threads = []

    next_cursor = None
    while True:
        try:
            result = client.conversations_history(
                channel=channel_id, cursor=next_cursor
            )
            print(result)
            messages = result["messages"]
            print(len(messages))
            for msg in messages:
                if "thread_ts" in msg and msg["thread_ts"] == msg["ts"]:
                    thread_result = client.conversations_replies(
                        channel=channel_id, ts=msg["ts"]
                    )
                    thread_messages = thread_result["messages"]
                    if thread_messages:
                        threads.append(thread_messages)
                elif (
                    "attachments" in msg
                    and msg["attachments"]
                    and "channel_id" in msg["attachments"][0]
                ):
                    threads.append([msg])
                elif "subtype" not in msg:
                    threads.append([msg])

            metadata = result["response_metadata"]
            if not metadata:
                break

            next_cursor = result["response_metadata"]["next_cursor"]
            if not next_cursor:
                break

        except SlackApiError as e:
            if e.response["error"] == "ratelimited":
                delay = int(e.response.headers.get("Retry-After", 1))
                print(f"Rate limited. Waiting {delay} seconds...")
                time.sleep(delay)
                continue
            raise e

    return threads


def get_user_info(user_id, team_name, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the users.info method using the WebClient
        result = client.users_info(user=user_id)

        return {
            "id": user_id,
            "name": result["user"]["name"],
            "is_bot": result["user"]["is_bot"],
            "internal": is_internal_user(result["user"]["team_id"], team_name),
        }

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_user_list(team_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    name_dict = {}
    try:
        # Call the users.info method using the WebClient
        result = client.users_list(team_id=team_id)

        for member in result["members"]:
            name_dict[member["id"]] = {
                "id": member["id"],
                "name": member["name"],
                "is_bot": member["is_bot"],
                "internal": True,
            }

        return name_dict

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_team_name(team_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the users.info method using the WebClient
        result = client.team_info(team=team_id)

        return result["team"]["name"]

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_root_url(channel_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the users.info method using the WebClient
        team_id = get_team_id(channel_id, slack_token)
        result = client.team_info(team=team_id)

        return result["team"]["url"]

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")


def get_channel(channel_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        result = client.conversations_info(channel=channel_id)
        return result["channel"]  # Returns the full channel object
    except SlackApiError as e:
        print(f"Error: {e.response['error']}")
        return None


def get_channel_ids_names(team_id, slack_token):
    client = get_slack_client(slack_token=slack_token)
    channel_mapping = {}

    try:
        next_cursor = None

        while True:
            response = client.conversations_list(
                team_id=team_id,
                types="public_channel,private_channel,mpim,im",
                limit=1000,
                cursor=next_cursor,
            )

            # Process current batch of channels
            for channel in response["channels"]:
                # For public/private channels, check is_member
                # For IMs and MPIMs, always include them
                if channel.get(
                    "is_member", True
                ):  # Default to True if is_member doesn't exist
                    # Handle different channel types (regular channels vs DMs)
                    if "name" in channel:
                        channel_mapping[channel["id"]] = channel["name"]

            # Check if there are more channels to fetch
            next_cursor = response.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break

        return channel_mapping

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")
        return {}


def is_internal_user(team_id, team_name, slack_token):
    client = get_slack_client(slack_token=slack_token)
    try:
        # Call the users.info method using the WebClient
        result = client.team_info(team=team_id)

        return result["team"]["name"] == team_name

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")
        return False


def replace_mentions(content, user_list):
    # Replace user mentions
    user_mentions = re.findall(r"<@([\dA-Z]+)>", content)
    for user_id in user_mentions:
        if user_id in user_list:
            content = content.replace(f"<@{user_id}>", f'@{user_list[user_id]["name"]}')

    return content


def extract_message_data(thread, user_list, root_url):
    out = []
    for msg in thread:
        try:
            if "user" not in msg:
                print(msg)
            if "user" in msg and msg["user"] in user_list:
                user_info = user_list[msg["user"]]
            else:
                user_info = {
                    "id": msg["user"] if "user" in msg else "",
                    "name": msg["username"] if "username" in msg else "",
                    "is_bot": False,
                    "internal": False,
                }
            if "reactions" in msg:
                reactions = msg["reactions"]
            else:
                reactions = []
            out.append(
                {
                    "attachments": msg["attachments"] if "attachments" in msg else [],
                    "user": user_info,
                    "timestamp": msg["ts"],
                    "text": replace_mentions(msg["text"], user_list),
                    "reactions": reactions,
                    "url": root_url + f"p{msg['ts'].replace('.','')}",
                }
            )
        except SlackApiError as e:
            print(f"Error extracting message data: {e.response['error']}")
            return None
    return out


def group_messages(messages, window_size):
    grouped_messages = []
    for i in range(len(messages)):
        start_index = max(i - window_size, 0)
        end_index = min(i + window_size + 1, len(messages))
        grouped_messages.append(messages[start_index:end_index])
    return grouped_messages


def prepare_thread_records(
    threads, channel_id, root_slack_url, user_list, team_name, team_id, channel_name
):
    records = []
    pages = group_messages(threads, 1)
    for i, thread in enumerate(threads):
        msg_blob = "".join([msg["text"] for msg in thread])
        url = root_slack_url + "archive/" + channel_id
        root = thread[0]
        thread_context = [
            extract_message_data(context_thread, user_list, root_slack_url)
            for context_thread in pages[i]
        ]
        records.append(
            {
                "record_id": get_content_hash(
                    format_thread_list(thread_context) + msg_blob
                ),
                "format": "md",
                "content": msg_blob,
                "attributes": {
                    "channel": json.dumps(
                        {"id": channel_id, "name": channel_name, "url": url}
                    ),
                    "thread": json.dumps(
                        extract_message_data(thread, user_list, root_slack_url)
                    ),
                    "team": json.dumps({"name": team_name, "id": team_id}),
                    "thread_context": json.dumps(thread_context),
                },
                "extra_attributes": {"content_hash": get_content_hash(msg_blob)},
            }
        )
    return records


@task(name="upload_slack_data")
def upload_slack_index(
    config,
    slack_token,
):
    records = []
    index_name = config["base_index_name"]
    channel_ids = config["input"].slack_info.channel_ids
    team_id = config["input"].slack_info.team_id
    source_id = config["input"].source_id
    org_alias = config["input"].org_alias
    project_id = config["input"].project_ids[0]
    team_name = get_team_name(team_id, slack_token=slack_token)
    print(f"team_name = {team_name}")
    root_url = get_root_url(channel_ids[0], slack_token=slack_token)
    user_list = get_user_list(team_id, slack_token=slack_token)
    channel_mapping = get_channel_ids_names(team_id, slack_token)
    channel_ids = set(channel_mapping.keys()) | set(channel_ids)
    print(f"channel_mapping = {channel_mapping}")
    for id, name in channel_mapping.items():
        print("processing channel: ", id)

        threads = get_channel_threads(id, slack_token=slack_token)
        print(f"number of threads = {len(threads)}, creating records")
        records += prepare_thread_records(
            threads, id, root_url, user_list, team_name, team_id, name
        )
        print(f"channel: {id} added to records")
    print(f"total records = {len(records)}")
    upload_batches(records, index_name, source_id, org_alias, project_id)


@flow(name="sync_slack_records_flow")
def sync_slack_records():
    orgs = ["devmarketing", "skyflow", "deliverlogic", "tossim", "deephavenio"]
    for org in orgs:
        configs = get_org_configs(org)
        for config in configs["slack"]:
            slack_info = config["input"].slack_info
            delete_mongodb_records_by_index_name(config["base_index_name"])
            if config["input"].source_id == "cm4k39a8c00a35er4oxu2vz73":
                slack_token = SLACK_TOKEN_MAPPING["deephavenio_internal"]
            else:
                slack_token = SLACK_TOKEN_MAPPING[org]
            upload_slack_index(
                config,
                slack_token,
            )
            create_source_sync_and_indexing_jobs(
                org,
                config["input"].source_id,
                status="PROCESSING",
                status_message="ingested",
            )
        trigger_auto_index_deploy(org, create_index_mapping(org, configs))

    # if __name__ == "__main__":
    # configs = get_org_configs("deephavenio", map_source_ids=True)
    # config = configs["cm4k39a8c00a35er4oxu2vz73"][0]


#     # channel = get_channel(
#     #     config["input"].slack_info.channel_ids[0], SLACK_TOKEN_MAPPING["tossim"]
#     # )
#     # print(f"channel_mapping = {channel}")
# channel_map = get_channel_ids_names(
#     config["input"].slack_info.team_id, SLACK_TOKEN_MAPPING["deephavenio_internal"]
# )

#     slack_token = SLACK_TOKEN_MAPPING["deephavenio_internal"]
#     slack_info = config["input"].slack_info
#     channel_ids = list(channel_map.keys())
#     upload_slack_index(
#         config["base_index_name"],
#         channel_ids,
#         config["input"].source_id,
#         "deephavenio",
#         config["input"].project_ids[0],
#         SLACK_TOKEN_MAPPING["deephavenio_internal"],
#     )
#     threads = get_channel_threads("C057KGB5UBZ")
#     for thread in threads:
