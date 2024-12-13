import asyncio
import json
import re
import traceback
from datetime import datetime
import typing as T

import discord
from discord.ext import commands
from prefect import flow, task

from engine_cloud.config import settings
from engine_cloud.utils.helpers import get_content_hash
from engine_cloud.utils.mongo import upload_batches, delete_mongodb_records_by_index_name
from engine_cloud.utils.helpers import get_org_configs, CreateIndexInput
from engine_cloud.gql.management_helpers import (
    get_all_sources_of_type,
    create_source_sync_and_indexing_jobs,
    get_source_sync_job_mapping,
    update_all_indexing_jobs,
)
from engine_cloud.processors.notion_helper import trigger_auto_index_deploy
from engine_cloud.processors.main import trigger_manual_index_deploy, create_index_mapping


intents = discord.Intents.default()
permissions = discord.Permissions.all()
intents.members = True
intents.message_content = True
intents.reactions = True

permissions.read_message_history = True


class DiscordClient(discord.Client):
    def __init__(
        self,
        server_id: str,
        channel_ids: T.List[str],
        team_member_roles: T.List[str],
        config_input: CreateIndexInput,
        indexing_engine: str,
    ) -> None:
        intents = discord.Intents.default()
        intents.members = True
        intents.message_content = True

        super().__init__(intents=intents)

        self.input_server_id = server_id
        self.input_channel_ids = channel_ids
        self.input_team_member_roles = team_member_roles
        self.config_input = config_input
        self.output_records = []
        self.indexing_engine = indexing_engine

    async def on_ready(self):
        print(f"Logged in as {self.user.name} - {self.user.id}")
        for guild in self.guilds:
            print(f"- {guild.name} (ID: {guild.id})")
        guild = self.get_guild(int(self.config_input.discord_info.server_id))
        if guild:
            for channel_id in self.config_input.discord_info.channel_ids:
                channel = guild.get_channel(int(channel_id))
                record_list = []
                if (
                    isinstance(channel, discord.TextChannel)
                    and self.indexing_engine == "DISCORD_MESSAGES"
                ):
                    channel_id = int(channel_id)
                    print(f"channel {channel_id} found in server channels")
                    all_messages = await get_all_messages(channel)
                    try:
                        if all_messages:
                            records = await create_message_records(
                                all_messages, channel, guild
                            )
                        record_list.extend(records)
                    except Exception as e:
                        traceback.print_exc()
                        raise (e)
                elif (
                    isinstance(channel, discord.ForumChannel)
                    and self.indexing_engine == "DISCORD_FORUM_POSTS"
                ):
                    print(f"channel {channel_id} is a forum channel")
                    if guild.id == "1156707934424809474":
                        continue
                    record_list.extend(await create_forum_records(channel))
                print("created records")
                upload_batches(
                    record_list,
                    self.config_input.base_index_name,
                    self.config_input.source_id,
                    self.config_input.org_alias,
                    self.config_input.project_ids[0],
                )
        await self.close()


def find_replies(all_messages, root_id):
    replies = []
    for message in all_messages:
        if message.reference and message.reference.message_id == root_id:
            replies.append(message)
    return replies


async def get_formatted_messages(all_messages, channel, guild):
    message_pairs = []
    for i, message in enumerate(all_messages):
        if not message.reference:
            page = group_messages(all_messages, 10, i)
            message_node = create_message_node(message, guild, False)
            # message_graph = await create_message_graph(page, channel, guild)
            structured_page = await structure_page(page, all_messages, channel, guild)
            formatted_message = format_message(message_node, 0, False)
            formatted_page = format_message_list(structured_page)
            message_pairs.append((formatted_message, formatted_page))
    return message_pairs


def get_forum_record_id(messages, thread_data):
    message_content = ""
    for message in messages:
        message_content += message["content"]
    return get_content_hash(thread_data["name"] + message_content)


@task(name="create_forum_records_tasks")
async def create_forum_records(channel):
    records = []
    try:
        archived_threads = channel.archived_threads(limit=10000)
        all_threads = []
        for thread in channel.threads:
            all_threads.append(thread)
        async for thread in archived_threads:
            all_threads.append(thread)
    except Exception as e:
        print(e)
        return []

    thread: discord.Thread
    for i, thread in enumerate(all_threads):
        thread_data = get_forum_thread_data(thread)
        construct_messages = []
        messages = thread.history(limit=None, oldest_first=True)
        async for message in messages:
            author_roles = []
            if isinstance(message.author, discord.Member):
                author_roles: T.List[str] = [role.name for role in message.author.roles]
            message_data = get_message_data(message, author_roles)
            construct_messages.append(message_data)

        for tag in thread.applied_tags:
            if tag.id == "1260363959006007329":
                continue

        record_id = get_forum_record_id(construct_messages, thread_data)
        records.append(
            {
                "record_id": record_id,
                "content": "",
                "url": "",
                "title": "",
                "attributes": {
                    "channel_type": "forum",
                    "thread": json.dumps(thread_data),
                    "history": json.dumps(construct_messages),
                },
                "extra_attributes": {},
            }
        )
    print(f"found {len(records)} records")
    return records


async def get_all_messages(channel):
    try:
        all_messages = []
        async for message in channel.history(
            oldest_first=True, limit=None, after=datetime(2024, 1, 1)
        ):
            if (
                message.type == discord.MessageType.default
                or message.type == discord.MessageType.reply
            ):
                # all_messages.append([message.author.id, str(message.created_at), message.content])
                all_messages.append(message)
        return all_messages
    except Exception as e:
        print(e)


def replace_mentions(message):
    content = message.content
    formats = {
        "<@": message.mentions,
        "<@!": message.mentions,
        "<#": message.channel_mentions,
        "<@&": message.role_mentions,
    }
    for format, mentions in formats.items():
        for value in mentions:
            if format == "<#" or format == "<@&":
                name = value.name
            else:
                name = value.display_name
            mention_syntax = f"{format}{value.id}>"
            content = content.replace(mention_syntax, f"{format[1]}{name}")

    pattern = r"<a?:[a-zA-Z0-9\_]+:[0-9]+>"
    content = re.sub(pattern, "", content)

    return content


def group_messages(messages, window_size, index):
    page = []
    start_index = max(index - window_size, 0)
    end_index = min(index + window_size + 1, len(messages))
    for i in range(start_index, end_index):
        if not messages[i].reference:
            page.append(messages[i])
    return page


def create_message_node(message, guild, is_child):
    message_record = {
        "message": create_message_record(message),
        "is_child": is_child,
        "children": [],
        "thread": None,
    }
    return message_record


async def structure_page(page, all_messages, channel, guild, child=False):
    structured_page = []
    for message in page:
        structure = await create_message_structure(
            message, all_messages, channel, guild, child
        )
        structured_page.append(structure)
    return structured_page


async def create_message_structure(
    root_message, all_messages, channel, guild, child=False
):
    # reverse the messages so that they are in chronological order
    root_record = create_message_node(root_message, guild, child)

    # Messages are in chronological order so if the reference message is not in the page we can ignore it because it was not fetched
    replies = find_replies(all_messages, root_message.id)
    root_record["children"] = await structure_page(
        replies, all_messages, channel, guild, child=True
    )

    flags = root_message.flags
    if flags.has_thread:
        thread = channel.get_thread(root_message.id)
        if thread:
            thread_messages = []
            async for message in thread.history(
                limit=None, oldest_first=True, after=datetime(2023, 1, 1)
            ):
                thread_messages.append(message)
            root_record["thread"] = await structure_page(
                thread_messages, all_messages, channel, guild
            )

    return root_record


def format_message_list(graph_list, level=0, thread=False):
    """
    Format a list of message graphs into a string for LLM processing.
    Args:
        Level: The indentation level of the message.
        Thread: Indicates if the message is part of a thread.
    """
    formatted_conversation = ""
    for graph in graph_list:
        formatted_conversation += format_message(graph, level, thread)
    return formatted_conversation


def format_message(message, level, thread):
    """
    Recursively format a message graph into a string for LLM processing.
    Args:
        Level: The indentaiton level of the message.
        Thread: Indicates if the message is part of a thread.
    """
    content = message["message"]["text"]
    author = message["message"]["user"]["name"]
    timestamp = datetime.utcfromtimestamp(message["message"]["timestamp"]).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    level = level + 1 if thread else level
    prefix = "  " * level
    formatted_message = f'{prefix}"""{content}""" from @{author} on {timestamp}\n'

    # Format the replies
    if message["children"]:
        formatted_message += prefix + "Replies:\n"
        formatted_message += format_message_list(message["children"], level + 1, thread)

    # Format the thread
    if message["thread"]:
        formatted_message += prefix + "Thread:\n"
        formatted_message += format_message_list(message["thread"], thread=True)

    return formatted_message


def create_message_record(message):
    if hasattr(message.author, "guild_permissions"):
        is_admin = message.author.guild_permissions.administrator
    else:
        is_admin = False

    return {
        "user": {
            "name": message.author.name,
            "id": message.author.id,
            "is_bot": message.author.bot,
            "is_admin": is_admin,
        },
        "timestamp": message.created_at.timestamp(),
        "id": str(message.id),
        "text": replace_mentions(message),
        "reactions": [str(reaction.emoji) for reaction in message.reactions],
        "url": message.jump_url,
    }


def get_message_blob(graph):
    blob = graph["message"]["text"]
    for child in graph["children"]:
        blob += get_message_blob(child)
    return blob


def get_context_blob(context):
    blob = ""
    for graph in context:
        blob += get_message_blob(graph)
    return blob


def get_forum_thread_data(thread):
    return {
        "id": thread.id,
        "parent_id": thread.parent_id,
        "parent": thread.parent.name if thread.parent else None,
        "owner_id": thread.owner_id,
        "owner": thread.owner.name if thread.owner else None,
        "name": thread.name,
        "type": thread.type.name,
        "last_message_id": thread.last_message_id,
        "last_message": (thread.last_message.content if thread.last_message else None),
        "message_count": thread.message_count,
        "member_count": thread.member_count,
        "created_at": (thread.created_at.isoformat() if thread.created_at else None),
        "category_id": thread.category_id,
        "category": (thread.category.name if thread.category else None),
        "archived": thread.archived,
        "archiver_id": thread.archiver_id,
        "jump_url": thread.jump_url,
    }


def get_message_data(message, author_roles):
    return {
        "id": message.id,
        "content": message.content,
        "clean_content": message.clean_content,
        "author_id": message.author.id,
        "author": message.author.name,
        "author_roles": author_roles,
        "author_is_bot": message.author.bot,
        "edited_at": (message.edited_at.isoformat() if message.edited_at else None),
        "application_id": message.application_id,
        "jump_url": message.jump_url,
        "reactions": [str(reaction.emoji) for reaction in message.reactions],
        "mentions": [mention.name for mention in message.mentions],
        "mention_everyone": message.mention_everyone,
        "pinned": message.pinned,
        "position": message.position,
        "type": message.type.name,
        "webhook_id": message.webhook_id,
    }


@task(name="create_message_records_task", log_prints=True)
async def create_message_records(all_messages, channel, guild):
    records = []
    for i in range(len(all_messages)):
        message = all_messages[i]
        try:
            if not message.reference and not message.author.bot:
                page = group_messages(all_messages, 10, i)
                graph = await create_message_structure(
                    message, all_messages, channel, guild
                )
                message_context = await structure_page(
                    page, all_messages, channel, guild
                )
                msg_blob = get_message_blob(graph)
                context_blob = get_context_blob(message_context)
                records.append(
                    {
                        "record_id": str(message.id),
                        "format": "md",
                        "content": msg_blob,
                        "attributes": {
                            "channel": json.dumps(
                                {
                                    "id": channel.id,
                                    "name": channel.name,
                                    "url": channel.jump_url,
                                }
                            ),
                            "message_graph": json.dumps(graph),
                            "message_context": json.dumps(message_context),
                            "serverId": guild.id,
                            "index_version": datetime.now().strftime(
                                "%Y-%m-%dT%H:%M:%S"
                            ),
                        },
                        "extra_attributes": {
                            "content_hash": get_content_hash(context_blob),
                        },
                    }
                )
        except Exception as e:
            print(e)
    print("returning records")
    return records


# @task(name="run_discord_pipeline_task", log_prints=True)
def run_discord_pipeline(org_alias):
    configs = get_org_configs(org_alias)
    if org_alias == "clerk":
        return
    if "discord" in configs:
        for config in configs["discord"]:
            print(f"found discord config for {org_alias}, {config['input'].source_id}")
            delete_mongodb_records_by_index_name(config["base_index_name"])
            client = DiscordClient(
                server_id=config["input"].discord_info.server_id,
                channel_ids=config["input"].discord_info.channel_ids,
                team_member_roles=[],
                config_input=config["input"],
                indexing_engine=config["input"].indexing_engine,
            )
            try:
                client.run(settings.DISCORD_BOT_TOKEN)
                print("bot closed")
            except KeyboardInterrupt:
                client.close()


@flow(name="run_discord_manual_sync_flow", log_prints=True)
def run_discord_pipeline_flow(org_alias):
    source_sync_jobs = get_source_sync_job_mapping(
        org_alias,
        status_requirement="PROCESSING",
        status_message_requirement="queued",
    )
    run_discord_pipeline(org_alias)
    configs = get_org_configs(org_alias)
    discord_configs = configs.get("discord", [])
    for config in discord_configs:
        source_id = config["input"].source_id
        jobs = source_sync_jobs.get(source_id, [])
        update_all_indexing_jobs(
            org_alias,
            jobs,
            status="PROCESSING",
            status_message="ingested",
        )
        trigger_manual_index_deploy(org_alias, configs)


@flow(name="run_discord_auto_sync_flow", log_prints=True)
def run_discord_sync():
    sources_map = get_all_sources_of_type("DiscordSource")
    for org, sources in sources_map.items():
        if not sources:
            continue
        print(org)
        run_discord_pipeline(org)
        configs = get_org_configs(org)
        for source in sources:
            try:
                create_source_sync_and_indexing_jobs(
                    org, source["id"], status="PROCESSING", status_message="ingested"
                )
            except:
                print(f"couldn't create jobs for {org} {source['id']}")
        index_mapping = create_index_mapping(
            org, configs, required_source_type="discord"
        )
        trigger_auto_index_deploy(org, index_mapping)


if __name__ == "__main__":
    run_discord_pipeline("resend")
