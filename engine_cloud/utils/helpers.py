import hashlib
import json
import subprocess
import pypandoc
import tiktoken
import html2text
import typing as T
import re
from typing import Union

from pydantic import BaseModel

from urllib.parse import urlparse
from engine_cloud.gql.management_helpers import (
    get_project_source_map_for_org,
    get_sources_with_project_ids,
)

NON_PROCESSED_TYPES = {"discord", "github", "api", "slack"}
NANGO_SOURCE_MAP = {
    "NotionSource": "notion",
    "ZendeskSupportTicketsSource": "zendesk_tickets",
}


def get_content_hash(content):
    """
    Get the hash of a string
    Args:
        content: the string to hash
    Returns:
        the hash of the string
    """
    raw_hash = hashlib.sha1(
        json.dumps({"html": content}, sort_keys=True).encode("utf-8")
    )
    return raw_hash.hexdigest()


def remove_indentation(md):
    """
    Remove indentation from a markdown string
    Args:
        md: the markdown string
    Returns:
        the markdown string with indentation removed
    """
    output = []

    for line in md.split("\n"):
        if line.lstrip().startswith("```"):
            line = line.lstrip()

        output.append(line)

    return "\n".join(output)


def get_owner_repo_from_github_url(url):
    parsed = urlparse(url)
    path = parsed.path
    elements = path.split("/")
    if "github.com" not in url:
        raise Exception("invalid GitHub url")
    if len(elements) < 3:
        raise Exception("GitHub url does not contain owner and repo")
    return elements[1], elements[2]


def convert_adoc_to_markdown(filename):
    command = [
        "npx",
        "downdoc",
        filename,
    ]
    subprocess.run(command, check=True)


def convert_rst_to_md(input_file, output_file):
    # Convert file
    output = pypandoc.convert_file(input_file, "md", format="rst")

    # Write to output file
    with open(output_file, "w") as f:
        f.write(output)

    print(f"Conversion successful: {output_file}")


def get_num_tokens(content):
    encoding = tiktoken.encoding_for_model("gpt-4")
    num_tokens = len(encoding.encode(content, disallowed_special=()))
    return num_tokens


class UrlMatchPattern(BaseModel):
    match_type: str = "regex"  # Either "regex" or "exact"
    pattern: str  # This will store either a regex or a full URL, depending on matchType


class CrawlerMatchingPatterns(BaseModel):
    exclude_patterns: T.List[UrlMatchPattern] = []
    include_patterns: T.List[UrlMatchPattern] = []


class CustomHTML2Text(html2text.HTML2Text):
    def handle_tag(self, tag, attrs, start):
        super().handle_tag(tag, attrs, start)
        # Add your custom tag handling
        if tag == "pre":
            if start:
                self.startpre = 1
                self.pre = 1
                self.p()
                self.o("```")  # start of a code block in markdown

            else:
                self.pre = 0
                self.o("```")  # end of a code block in markdown
            self.p()


class SlackInfo(BaseModel):
    team_id: str
    channel_ids: T.List[str]


class DiscordInfo(BaseModel):
    server_id: str
    channel_ids: T.List[str]
    team_member_roles: T.List[str] = []


class NangoInfo(BaseModel):
    connection_id: str


class GithubInfo(BaseModel):
    repo: str
    owner: str
    includeDiscussions: bool = False
    includePullRequests: bool = False
    includeREADMEs: bool = False
    includeReleaseNotes: bool = False
    includeIssues: bool = False
    includeSourceCode: bool = False


class CreateIndexInput(BaseModel):
    index_name: str  # source index from management with the date removed
    organization: str  # display name of the organization
    source_type: str  # site, docs, discord, github, discourse, slack, support
    org_alias: str = ""  # mgmt or alias
    root_sitemap_url: str = (
        ""  # if there is a sitemap url that links to other sitemap urls
    )
    crawler_start_urls: T.List[str] = []  # Urls to crawl from
    crawler_matching_patterns: CrawlerMatchingPatterns = (
        CrawlerMatchingPatterns()
    )  # Include/Exclude url patterns
    github_info: Union[GithubInfo, None] = None
    discord_info: Union[DiscordInfo, None] = None
    slack_info: Union[SlackInfo, None] = None
    nango_info: Union[NangoInfo, None] = None
    base_index_name: str = ""  # I should eventually move all configs to use this
    processed_index_name: str = ""
    source_id: str = ""
    project_ids: T.List[str] = []
    indexing_engine: str = ""  # indexing engine from mgmt
    auto_refresh_enabled: bool = True
    structured_type: bool = False


class ManualFilterParams(BaseModel):
    xpath: Union[str, None] = None
    tagType: Union[str, None] = None
    classValues: T.List[str] = []


class ManualFilterInput(BaseModel):
    tags: T.List[ManualFilterParams]


class DateParams(BaseModel):
    xpath: str
    dateFormat: str = "%B %d, %Y"
    dateIndex: int = None


class DateParamInput(BaseModel):
    params: T.List[DateParams]


github_filter_params = ManualFilterInput(
    tags=[
        ManualFilterParams(
            tagType="div",
            classValues=[
                "clearfix",
                "container-xl",
                "px-3",
                "px-md-4",
                "px-lg-5",
                "mt-4",
            ],
        )
    ]
)


def remove_date(index_id):
    return "_".join(index_id.split("_")[:-1])


def create_matching_patterns(source):
    title_patterns = CrawlerMatchingPatterns()
    url_patterns = CrawlerMatchingPatterns()
    if "urlMatchingPatterns" in source:
        url_patterns = CrawlerMatchingPatterns(
            exclude_patterns=[
                UrlMatchPattern(match_type=x["matchType"], pattern=x["pattern"])
                for x in source["urlMatchingPatterns"]["excludePatterns"]
            ],
            include_patterns=[
                UrlMatchPattern(match_type=x["matchType"], pattern=x["pattern"])
                for x in source["urlMatchingPatterns"]["includePatterns"]
            ],
        )
    if "titleMatchingPatterns" in source:
        title_patterns = CrawlerMatchingPatterns(
            exclude_patterns=[
                UrlMatchPattern(match_type=x["matchType"], pattern=x["pattern"])
                for x in source["titleMatchingPatterns"]["excludePatterns"]
            ],
            include_patterns=[
                UrlMatchPattern(match_type=x["matchType"], pattern=x["pattern"])
                for x in source["titleMatchingPatterns"]["includePatterns"]
            ],
        )
    return url_patterns, title_patterns


def management_to_ingester_inputs(
    org_alias, org, project_id=None, map_source_ids=False
):
    if not org_alias:
        org_alias = org

    sources = get_sources_with_project_ids(org_alias)

    inputs = {}

    for source in sources:
        if not source:
            continue
        if not source["indexes"]:
            continue
        for source_index in source["indexes"]:
            index_name = remove_date(source_index["id"])
            url_patterns, title_patterns = create_matching_patterns(source)
            if source["__typename"] == "GeneralWebSource":
                source_type = "site" if not source["isDocumentation"] else "docs"
                key = source["id"] if map_source_ids else source_type
                inputs.setdefault(key, []).append(
                    CreateIndexInput(
                        source_type="site" if not source["isDocumentation"] else "docs",
                        organization=org,
                        org_alias=org_alias,
                        index_name=index_name,
                        sitemap_urls=source["crawlerSitemapUrls"],
                        ingest_urls=source["ingestUrls"],
                        crawler_start_urls=source["crawlerStartUrls"],
                        crawler_matching_patterns=url_patterns,
                        title_matching_patterns=title_patterns,
                        filter_params=ManualFilterInput(
                            tags=[
                                ManualFilterParams(xpath=xpath)
                                for xpath in source["filterXPaths"]
                            ]
                        ),
                        date_params=DateParamInput(
                            params=[
                                DateParams(xpath=xpath)
                                for xpath in source["dateXPaths"]
                            ]
                        ),
                        source_id=source["id"],
                        project_id=project_id,
                        indexing_engine=source_index["indexingEngine"],
                        auto_refresh_enabled=source["isAutoRefreshEnabled"],
                    )
                )
            if source["__typename"] == "GitHubSource":
                key = source["id"] if map_source_ids else "github"
                if (
                    source["includeReleaseNotes"]
                    and source_index["indexingEngine"] == "GITHUB_RELEASES"
                ):
                    index_name = f"{org_alias}_github-releases-{source['owner']}-{source['repo']}"
                    github_input = GithubInfo(
                        repo=source["repo"],
                        owner=source["owner"],
                        includeReleaseNotes=True,
                    )
                    inputs.setdefault(key, []).append(
                        CreateIndexInput(
                            source_type="github",
                            organization=org,
                            org_alias=org_alias,
                            index_name=index_name,
                            github_info=github_input,
                            source_id=source["id"],
                            project_ids=source["project_ids"],
                            filter_params=github_filter_params,
                            indexing_engine=source_index["indexingEngine"],
                            auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        )
                    )
                if (
                    source["includeDiscussions"]
                    and source_index["indexingEngine"] == "GITHUB_DISCUSSIONS"
                ):
                    index_name = f"{org_alias}_github-discussions-{source['owner']}-{source['repo']}"
                    github_input = GithubInfo(
                        repo=source["repo"],
                        owner=source["owner"],
                        includeDiscussions=True,
                    )
                    inputs.setdefault(key, []).append(
                        CreateIndexInput(
                            source_type="github",
                            organization=org,
                            org_alias=org_alias,
                            index_name=index_name,
                            github_info=github_input,
                            source_id=source["id"],
                            project_ids=source["project_ids"],
                            filter_params=github_filter_params,
                            indexing_engine=source_index["indexingEngine"],
                            auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        )
                    )
                if (
                    source["includeREADMEs"]
                    and source_index["indexingEngine"] == "GENERIC_DOCS"
                    or source_index["indexingEngine"] == "GITHUB_READMES"
                ):
                    index_name = (
                        f"{org_alias}_github-readmes-{source['owner']}-{source['repo']}"
                    )
                    github_input = GithubInfo(
                        repo=source["repo"],
                        owner=source["owner"],
                        includeREADMEs=True,
                    )
                    inputs.setdefault(key, []).append(
                        CreateIndexInput(
                            source_type="github",
                            organization=org,
                            org_alias=org_alias,
                            index_name=index_name,
                            github_info=github_input,
                            source_id=source["id"],
                            project_ids=source["project_ids"],
                            filter_params=github_filter_params,
                            indexing_engine=source_index["indexingEngine"],
                            auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        )
                    )
                if (
                    source["includeIssues"] or source["includePullRequests"]
                ) and source_index["indexingEngine"] == "GITHUB_ISSUES":
                    github_input = GithubInfo(
                        repo=source["repo"],
                        owner=source["owner"],
                        includeIssues=True,
                    )
                    index_name = (
                        f"{org_alias}_github-issues-{source['owner']}-{source['repo']}"
                    )
                    inputs.setdefault(key, []).append(
                        CreateIndexInput(
                            source_type="github",
                            organization=org,
                            org_alias=org_alias,
                            index_name=index_name,
                            github_info=github_input,
                            source_id=source["id"],
                            project_ids=source["project_ids"],
                            filter_params=github_filter_params,
                            indexing_engine=source_index["indexingEngine"],
                            auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        )
                    )
                if (
                    source["includeSourceCode"]
                    and source_index["indexingEngine"] == "GENERIC_DOCS"
                ):
                    index_name = f"{org_alias}_github-examples-{source['owner']}-{source['repo']}"
                    github_input = GithubInfo(
                        repo=source["repo"],
                        owner=source["owner"],
                        includeSourceCode=True,
                    )
                    inputs.setdefault(key, []).append(
                        CreateIndexInput(
                            source_type="github",
                            organization=org,
                            org_alias=org_alias,
                            index_name=index_name,
                            github_info=github_input,
                            source_id=source["id"],
                            project_ids=source["project_ids"],
                            filter_params=github_filter_params,
                            indexing_engine=source_index["indexingEngine"],
                            crawler_matching_patterns=url_patterns,
                            auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        )
                    )

            elif source["__typename"] == "DiscordSource":
                index_name = remove_date(source_index["id"])
                key = source["id"] if map_source_ids else "discord"
                discord_info = DiscordInfo(
                    server_id=source["serverId"],
                    channel_ids=source["channelIds"],
                    team_member_roles=source["teamMemberRoles"],
                )
                inputs.setdefault(key, []).append(
                    CreateIndexInput(
                        source_type="discord",
                        organization=org,
                        org_alias=org_alias,
                        index_name=index_name,
                        source_id=source["id"],
                        discord_info=discord_info,
                        project_ids=source["project_ids"],
                        indexing_engine=source_index["indexingEngine"],
                        base_index_name=f"inkeep_{index_name}",
                        auto_refresh_enabled=source["isAutoRefreshEnabled"],
                    )
                )
            elif source["__typename"] == "SlackSource":
                print("found")
                index_name = remove_date(source_index["id"])
                key = source["id"] if map_source_ids else "slack"
                slack_info = SlackInfo(
                    team_id=source["workspaceId"], channel_ids=source["channelIds"]
                )
                inputs.setdefault(key, []).append(
                    CreateIndexInput(
                        source_type="slack",
                        organization=org,
                        org_alias=org_alias,
                        index_name=index_name,
                        slack_info=slack_info,
                        source_id=source["id"],
                        project_ids=source["project_ids"],
                        indexing_engine=source_index["indexingEngine"],
                        base_index_name=f"inkeep_{index_name}",
                        auto_refresh_enabled=source["isAutoRefreshEnabled"],
                    )
                )
            elif source["__typename"] in NANGO_SOURCE_MAP:
                index_name = remove_date(source_index["id"])
                key = (
                    source["id"]
                    if map_source_ids
                    else NANGO_SOURCE_MAP[source["__typename"]]
                )
                inputs.setdefault(key, []).append(
                    CreateIndexInput(
                        source_type=NANGO_SOURCE_MAP[source["__typename"]],
                        organization=org,
                        org_alias=org_alias,
                        index_name=index_name,
                        nango_info=NangoInfo(connection_id=source["connectionId"]),
                        auto_refresh_enabled=source["isAutoRefreshEnabled"],
                        base_index_name=f"inkeep_{index_name}",
                        source_id=source["id"],
                        project_ids=source["project_ids"],
                    )
                )
    return inputs


def generate_config(inputs: T.Dict, map_to_source_id=False):
    config = {}
    for source_type, input_list in inputs.items():
        for input in input_list:
            if input.source_type in NON_PROCESSED_TYPES:
                base_index_name = f"inkeep_{input.index_name}"
                processed_index_name = f"inkeep_{input.index_name}"
            else:
                base_index_name = f"inkeep_base_{input.index_name}"
                processed_index_name = f"inkeep_processed_{input.index_name}"
            key = source_type if not map_to_source_id else input.source_id
            config.setdefault(key, []).append(
                {
                    "base_index_name": base_index_name,
                    "processed_index_name": processed_index_name,
                    "input": input,
                }
            )

    return config


def get_org_configs(org, org_alias="", project_id="", map_source_ids=False):
    if org_alias:
        return generate_config(
            management_to_ingester_inputs(
                org_alias, org, project_id=project_id, map_source_ids=map_source_ids
            ),
            map_to_source_id=map_source_ids,
        )
    else:
        return generate_config(
            management_to_ingester_inputs(
                org, org, project_id=project_id, map_source_ids=map_source_ids
            ),
            map_to_source_id=map_source_ids,
        )


def get_url_from_md(text):
    pattern = r"\[.*?\]\((https?://\S+)\)"

    match = re.search(pattern, text)
    if match:
        url = match.group(1)
        return url
    else:
        return None


def preprocess_url(url):
    """
    Preprocess a url to remove www. and trailing slashes
    Args:
        url: the url to preprocess
    Returns:
        the preprocessed url
    """

    parsed_url = urlparse(url.strip().lstrip())
    scheme = parsed_url.scheme
    if scheme not in ["http", "https"]:
        raise ValueError(f"Invalid scheme: {scheme}")
    # netloc = parsed_url.netloc.replace("www.", "")
    netloc = parsed_url.netloc.lower()
    # netloc = netloc.lower()
    path = parsed_url.path.rstrip("/")
    preprocessed_url = f"{scheme}://{netloc}{path}"
    return preprocessed_url


if __name__ == "__main__":
    print(get_project_source_map_for_org("pinecone"))
