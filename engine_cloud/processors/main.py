import base64
import time
from datetime import datetime
import jwt
import requests
import re
from datetime import datetime, timedelta

import requests
import pycmarkgfm
import nbformat
import json
from nbconvert import HTMLExporter
from time import sleep
import backoff
import os
from prefect import task, flow
from prefect.docker import DockerImage
from prefect.deployments import run_deployment


from engine_cloud.config import settings
from engine_cloud.utils.helpers import (
    get_content_hash,
    remove_indentation,
    get_owner_repo_from_github_url,
    convert_adoc_to_markdown,
    convert_rst_to_md,
    get_num_tokens,
    get_org_configs,
    remove_date,
    preprocess_url,
    GithubInfo,
    CustomHTML2Text,
    CreateIndexInput,
)
from engine_cloud.utils.mongo import (
    delete_mongodb_records_by_metadata,
    get_mongo_records_by_metadata,
    upload_batches,
    MongoRecordMetadataQueryInput,
    IndexingEngine,
)

from engine_cloud.utils.page_graph import create_graph_records
from engine_cloud.gql.management_helpers import (
    get_sources_for_org,
    get_active_orgs,
    get_most_recent_sync_time,
    create_source_sync_and_indexing_jobs,
    get_source_sync_job_mapping,
    update_all_indexing_jobs,
    fail_indexing_jobs,
    get_source,
)


AUTO_EXCLUDE_EXTENSIONS = {"png", "ico", "svg", "css", ".gitignore"}

README_EXTENSIONS = {"md", "rst", "adoc"}

NON_GRAPH_TYPES = {
    "GITHUB_ISSUES",
    "GITHUB_DISCUSSIONS",
    "DISCORD_FORUM_POSTS",
    "DISCOURSE_POSTS",
    "OPENAPI",
    "DISCORD_MESSAGES",
    "SLACK_MESSAGES",
}

PROD_SEARCH_CONFIG = {
    "page_size": 100,
    "page_limit": 2000,
}

ORGS_WITH_GITHUB = [
    "temboio",
    "reflex",
    "nixtla",
    "spacetimedb",
    "hookdeck",
    "captions",
    "speakeasy",
    "gentrace",
    "securiti",
    "arangodb",
    "teleport",
    "emqx",
    "chakra-ui",
    "inkeep",
    "pinecone",
    "authjs",
    "twilio",
    "novu",
    "astronomer",
    "staffplus",
    "windmill",
    "resend",
    "rollbar",
    "giantswarm",
    "drizzle",
    "guildquality",
    "bun",
    "oso",
    "activepieces",
    "slashid",
    "lemonsqueezy",
    "shake",
    "contentful",
    "xano",
    "postman",
    "grit",
    "ycombinator",
    "greyco",
    "prefect",
    "payabli",
    "zuplo",
    "cloudmydc",
    "vapi",
    "styra",
    "knock",
    "sciphi",
    "springhealth",
    "baseten",
    "hivemq",
    "blandai",
    "weaviate",
    "tosspayments",
    "block",
    "clerk",
    "atopileio",
    "propelauth",
    "pdq",
    "piecesapp",
    "endorlabs",
    "anthropic",
    "ably",
    "scale",
    "tabtravel",
    "stytch",
    "render",
    "freshpaintio",
    "svix",
    "intelycare",
    "nango",
    "shuttlehq",
    "pubnub",
    "workos",
    "raftlabs",
    "runswiftapp",
    "evercommerce",
    "descope",
    "syntagai",
    "sailpoint",
    "warpdev",
    "opuspro",
    "recallai",
    "highlightio",
    "connecteam",
    "bytio",
    "owndata",
    "humeai",
    "syncmyworkout",
    "missioncloud",
    "alkira",
    "chainguarddev2",
    "infobip",
    "onestream",
    "mappedin",
    "snowflake",
    "wancloudsnet",
    "outsystems",
    "turnoutnow",
    "kinde",
    "tanaza",
    "sisense",
    "skyflow",
    "tinybirdco",
    "camunda",
    "viam",
    "tanium",
    "trigger",
    "developerspotify",
    "liblab",
    "instruqt",
    "rootly",
    "appsmith",
    "saucelabs",
    "solace",
    "appsflyer",
    "astronomerio",
    "copilotkitai",
    "atomicfinancial",
    "chromatic",
    "deel",
    "dataiku",
    "hubspot",
    "fastly",
    "techywebsolutions",
    "mergedev",
    "coalesceio",
    "posthog",
    "flyio",
    "vercel-clone",
    "dittolive",
    "directusio",
    "smartmca",
    "trunkio",
    "cerebriumai",
    "affixapi",
    "tooeasy",
    "floxdev",
    "flutterflowio",
    "neon-clone",
    "doc-detective",
    "toss-internal",
    "tossim",
    "rapydnet",
    "kubefirstio",
    "discordapp",
    "octolane",
    "okta",
    "ostechnix",
    "fingerprint",
    "tailcallrun",
    "openqco",
    "startreeai",
    "particleio",
    "aerospike",
    "zilliz-clone",
    "openxcell",
    "singlestore-clone",
    "tightknitai",
    "stackabletech",
    "worldline",
    "neon",
]

language_map = {
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".java": "java",
    ".py": "python",
    ".rb": "ruby",
    ".html": "html",
    ".css": "css",
    ".js": "javascript",
    ".ts": "typescript",
    ".php": "php",
    ".swift": "swift",
    ".dart": "dart",
    ".cmake": "cmake",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".xml": "xml",
    ".m": "objective-c",
    ".mm": "objective-cpp",
    ".kt": "kotlin",
    ".xcworkspacedata": "xml",
    ".plist": "xml",
    ".pbxproj": "xml",
    ".xcscheme": "xml",
    ".php": "php",
    ".html": "html",
    # Add more file extensions and their corresponding languages as needed
}

APP_ID = "926466"
PRIVATE_KEY_PATH = "inkeep-ingestion.2024-06-20.private-key.pem"

INSTALLATION_ID_MAPPING = {
    "devcode-git/PaymentIQ-Documentation-Portal/": 55687485,
    "neondatabase/cloud/": 56135006,
    "neondatabase/docs/": 56135006,
}


def create_index_mapping(org, configs, required_source_type=None):
    index_mapping = {}
    if type(configs) != dict:
        configs = {str(i): [config] for i, config in enumerate(configs)}
    for source_type, config_list in configs.items():
        if required_source_type and source_type != required_source_type:
            print("skip")
            continue
        for config in config_list:
            source_index_mapping = {}
            source = get_source(config["input"].source_id, org)
            for index in source["indexes"]:
                if config["input"].index_name == remove_date(index["id"]):
                    if (
                        config["input"].source_type == "docs"
                        and config["input"].structured_type
                    ):
                        source_index_mapping[index["id"]] = []
                    else:
                        if (
                            "_api_" in index["id"]
                            or "_backendapi_" in index["id"]
                            or "_frontendapi_" in index["id"]
                        ):
                            source_index_mapping[index["id"]] = [
                                f"inkeep_{remove_date(index['id'])}"
                            ]
                        elif source["__typename"] == "ZendeskSupportTicketsSource":
                            source_index_mapping[index["id"]] = [
                                config["base_index_name"]
                            ]
                        elif (
                            index["indexingEngine"] not in NON_GRAPH_TYPES
                            and "_github" not in index["id"]
                        ):
                            source_index_mapping[index["id"]] = [
                                f"{config['processed_index_name']}_graph"
                            ]
                        else:
                            source_index_mapping[index["id"]] = [
                                config["processed_index_name"]
                            ]
            index_mapping.setdefault(config["input"].source_id, {}).update(
                source_index_mapping
            )
    return index_mapping


@backoff.on_exception(backoff.constant, Exception, max_tries=5, interval=1)
def ping_source_sync_indexer(org, configs, maintenance=False):
    print("pinging source sync indexer")
    index_mapping = create_index_mapping(org, configs)
    body = {"index_mapping": index_mapping, "org_alias": org}
    print(index_mapping)
    request_body = {
        "state": {
            "type": "SCHEDULED",
            "name": "Index Flow",
            "message": "starting run",
        },
        "name": "index_sync_job_flow",
        "parameters": {"input": body},
    }

    response = requests.post(
        settings.INDEX_AUTO_SOURCE_SYNC_URL,
        json=request_body,
    )
    response.close()

    print(f"status code is: {response.status_code}")
    if response.status_code == 503:
        print(response.text)
        raise Exception("failed to ping indexer")
    return response


# Function to generate JWT
def generate_jwt(app_id, private_key_path):
    now = datetime.utcnow()
    payload = {
        "iat": now,
        "exp": now + timedelta(seconds=600),  # JWT expiration time (10 minute maximum)
        "iss": app_id,
    }
    with open(private_key_path, "rb") as f:
        private_key = f.read()
    return jwt.encode(payload, private_key, algorithm="RS256")


# Function to list installations
def list_installations():
    jwt_token = generate_jwt(APP_ID, PRIVATE_KEY_PATH)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    return response.json()


# Function to get installation access token
def get_installation_access_token(installation_id):
    try:
        jwt_token = generate_jwt(APP_ID, PRIVATE_KEY_PATH)
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.post(
            f"https://api.github.com/app/installations/{installation_id}/access_tokens",
            headers=headers,
        )
        response.raise_for_status()
        return response.json()["token"]
    except Exception as e:
        print(f"Error getting installation access token: {str(e)}")
        raise


@backoff.on_exception(backoff.constant, Exception, max_tries=5, interval=1)
def fetch_contents(url, params={"per_page": 100}, access_token="", json=True):
    for url_fragment, installation_id in INSTALLATION_ID_MAPPING.items():
        if url_fragment in url:
            print("using access token")
            token = get_installation_access_token(installation_id)
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
            }
            print(url)
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            if json:
                return response.json()
            else:
                return response

    try:
        headers = {
            "Authorization": f"bearer {settings.GITHUB_TOKEN}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except Exception as e:
        if "403 Client Error: Forbidden for url" in str(e):
            headers = {
                "Authorization": f"bearer {settings.GITHUB_TOKEN_ALTERNATE}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
        else:
            raise e

    return response.json()


def get_repos_for_owner(owner):
    # Initialize variables
    repos = []
    page = 1
    base_url = f"https://api.github.com/orgs/{owner}/repos"

    while True:
        # Loop to handle pagination
        params = {
            "type": "public",
            "sort": "full_name",
            "per_page": 100,  # max allowed per page
            "page": page,
        }

        data = fetch_contents(base_url, params=params)
        if not data:  # if no more data is returned
            break

        # Append current page of repos to the list
        repos.extend(data)
        page += 1  # increment to fetch next page

    sorted_repos = sorted(
        repos, key=lambda repo: repo["stargazers_count"], reverse=True
    )

    return [repo["html_url"] for repo in sorted_repos][: min(4, len(sorted_repos))]


def get_num_github_stars(owner, repo):
    response = fetch_contents(f"https://api.github.com/repos/{owner}/{repo}")
    return response["stargazers_count"]


def get_top_four_repos_by_stars(urls):
    star_map = {}

    if len(urls) <= 4:
        return urls

    for url in urls:
        owner, repo = get_owner_repo_from_github_url(url)
        stars = get_num_github_stars(owner, repo)
        star_map[stars] = url

    top = sorted(star_map.items(), key=lambda item: item[1], reverse=True)[:4]

    return [item[1] for item in top]


def get_last_updated_date(owner, repo):
    response = fetch_contents(f"https://api.github.com/repos/{owner}/{repo}")
    return response["updated_at"]


def filter_old_repos(urls):
    filtered = []
    for url in urls:
        owner, repo = get_owner_repo_from_github_url(url)
        updated_at = get_last_updated_date(owner, repo)
        if "2024" in updated_at:
            filtered.append(url)
    return filtered


def matches_readme_extension(url: str):
    for extension in README_EXTENSIONS:
        print(url)
        if url.endswith(extension):
            print("matches")
            return True
    return False


def fetch_readme_content(readme_url):
    content_data = fetch_contents(readme_url)
    content_encoded = content_data["content"]
    content_decoded = base64.b64decode(content_encoded).decode("utf-8")
    return content_decoded


def search_directory_for_readmes(dir_url, readme_mapping):
    contents = fetch_contents(dir_url)
    for item in contents:
        if (
            item["type"] == "file"
            and item["name"].lower().startswith("readme")
            and matches_readme_extension(item["path"])
        ):
            readme_content = fetch_readme_content(item["url"])
            # Add to mapping
            readme_mapping[item["html_url"]] = readme_content
        elif item["type"] == "dir":
            # Recursively search in subdirectories
            search_directory_for_readmes(item["url"], readme_mapping)


def search_directory_recursive(
    dir_url,
    inclusion_filetypes=[],
    exclusion_filetypes=[],
    inclusion_filepaths=[],
    exclusion_filepaths=[],
    nb_files=[],
    path="",
    access_token="",
):
    contents = fetch_contents(dir_url, access_token=access_token)
    for item in contents:
        if item["type"] == "dir":
            # Recurse into directories
            search_directory_recursive(
                item["url"],
                inclusion_filetypes=inclusion_filetypes,
                exclusion_filetypes=exclusion_filetypes,
                inclusion_filepaths=inclusion_filepaths,
                exclusion_filepaths=exclusion_filepaths,
                nb_files=nb_files,
                path=path + item["name"] + "/",
                access_token=access_token,
            )

        if not inclusion_filetypes:
            filetype_check = True

        for filetype in inclusion_filetypes:
            if "/" in filetype and re.search(filetype, item["html_url"]):
                filetype_check = True
                break
            if item["name"].endswith(
                "." + filetype
            ):  # and "old_" not in item["name"] and "examples" in item["html_url"]:
                filetype_check = True
                break
            else:
                filetype_check = False

        for filetype in exclusion_filetypes:
            if "/" in filetype and re.search(filetype, item["html_url"]):
                filetype_check = False
                break
            if item["name"].endswith("." + filetype):
                filetype_check = False
                break

        if "firecrawl" in item["html_url"]:
            if "examples" in item["html_url"]:
                filetype_check = True
            else:
                filetype_check = False

        if item["type"] == "file" and filetype_check:
            try:
                content_data = fetch_contents(item["url"])
                print("FETCHING CONTENT")
                if isinstance(content_data, dict) and "content" in content_data:
                    content = base64.b64decode(content_data["content"]).decode("utf-8")
                else:
                    print(f"Unexpected response format for {item['url']}")
                    continue
            except Exception as e:
                print("FILE NOT FOUND", e)
                continue
            nb_files.append(
                {
                    "path": item["path"],
                    "url": item["html_url"],
                    "content": content,
                    "name": item["name"],
                }
            )

    return nb_files


def find_readme_title(readme_content):
    readme_content = readme_content.strip()
    lines = readme_content.split("\n")
    for line in lines:
        if line.startswith("#"):
            return line.replace("#", "").strip()
    return ""


def gfm_conversion(gfm):
    html = pycmarkgfm.gfm_to_html(gfm, options=pycmarkgfm.options.unsafe)
    md = CustomHTML2Text().handle(html)
    return md


def run_adoc_helper(content):
    with open("adoc_readme.adoc", "w") as f:
        f.write(content)
    convert_adoc_to_markdown("adoc_readme.adoc")
    with open("adoc_readme.md", "r") as f:
        return f.read()


def run_rst_helper(content):
    with open("rst_readme.rst", "w") as f:
        f.write(content)
    convert_rst_to_md("rst_readme.rst", "rst_readme.md")
    with open("rst_readme.md", "r") as f:
        return f.read()


def create_readme_records(owner, repo):
    readme_mapping = {}
    records = []
    exclusion_folders = []
    if repo == "reflex":
        exclusion_folders = [
            "/de/",
            "/es/",
            "/in/",
            "/it/",
            "/ja/",
            "/kr/",
            "/pe/",
            "/kr/",
            "/pt/pt_br/",
            "/tr/",
            "/zh/",
        ]
    root_url = f"https://api.github.com/repos/{owner}/{repo}/contents"
    search_directory_for_readmes(root_url, readme_mapping)
    for url, content in readme_mapping.items():
        if url.endswith(".md"):
            content = gfm_conversion(content)
        elif url.endswith(".rst"):
            content = run_rst_helper(content)
        elif url.endswith("adoc"):
            content = run_adoc_helper(content).replace("```adoc", "")
        skip = False
        for val in exclusion_folders:
            if val in url:
                skip = True
        if skip:
            print("skipping")
            continue

        records.append(
            {
                "record_id": get_content_hash(content),
                "format": "md",
                "content": content,
                "url": url,
                "title": find_readme_title(content),
                "attributes": {
                    "hash_html": "",
                    "hash_content": get_content_hash(content),
                    "num_tokens": get_num_tokens(content),
                    "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "record_type": "site",
                },
                "extra_attributes": {},
            }
        )
    return records


def create_entire_repo_records(
    owner,
    repo,
    inclusion_filetypes=[],
    exclusion_filetypes=[],
    inclusion_filepaths=[],
    exclusion_filepaths=[],
    access_token="",
):
    root_url = f"https://api.github.com/repos/{owner}/{repo}/contents"
    files = search_directory_recursive(
        root_url,
        inclusion_filetypes=inclusion_filetypes,
        exclusion_filetypes=exclusion_filetypes,
        inclusion_filepaths=inclusion_filepaths,
        exclusion_filepaths=exclusion_filepaths,
        access_token=access_token,
    )
    records = []
    for file in files:
        content = f"# {file['path']}\n"
        extension = file["name"].split(".")[-1]
        language = language_map.get(extension, extension)

        if extension in AUTO_EXCLUDE_EXTENSIONS:
            continue

        if extension == ".md":
            content += file["content"]
        else:
            content += f"```{language}\n{file['content']}\n```"

        filepath_match = False if inclusion_filepaths else True
        for inclusion_filepath in inclusion_filepaths:
            if inclusion_filepath in file["path"]:
                filepath_match = True

        for exclusion_filepath in exclusion_filepaths:
            if exclusion_filepath in file["path"]:
                filepath_match = False

        if not filepath_match:
            continue

        if "README" in file["url"]:
            continue

        records.append(
            {
                "record_id": get_content_hash(content),
                "format": "md",
                "content": content,
                "url": file["url"],
                "title": f"{owner}/{repo}/{file['path']}",
                "attributes": {
                    "hash_html": "",
                    "path": f"{owner}/{repo}/{file['path']}",
                    "hash_content": get_content_hash(content),
                    "num_tokens": get_num_tokens(content),
                    "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "record_type": "site",
                },
                "extra_attributes": {},
            }
        )
    return records


def get_min_path(paths):
    curr_path = []
    for path in paths:
        path_split = path.split("/")
        if not curr_path:
            curr_path = path_split
        else:
            overlap = set(curr_path).intersection(set(path_split))
            new_path = []
            for item in path_split:
                if item in overlap:
                    new_path.append(item)
            curr_path = new_path
    return "/".join(curr_path)


def get_blob_record(
    component_records, content_blob, num_tokens, curr_blob_urls, owner="", repo=""
):
    min_path = get_min_path(
        record["attributes"]["path"] for record in component_records
    )
    min_path_split = min_path.split("/")
    if owner and repo:
        url = f"https://github.com/{owner}/{repo}/tree/main"
    else:
        url = "https://stytch.com/docs"
    if len(min_path_split) > 2:
        end = "/".join(min_path_split[2:])
        url = url + "/" + end

    return {
        "record_id": get_content_hash(content_blob),
        "format": "md",
        "content": content_blob,
        "url": url,
        "title": min_path,
        "attributes": {
            "hash_html": "",
            "path": min_path,
            "combined_urls": json.dumps(curr_blob_urls),
            "hash_content": get_content_hash(content_blob),
            "num_tokens": num_tokens,
            "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "record_type": "site",
        },
        "extra_attributes": {},
    }


def get_repo_blob(records, owner="", repo=""):
    filtered = [
        record
        for record in records
        if ".gitignore" not in record["attributes"]["path"]
        and "LICENSE" not in record["attributes"]["path"]
        and "CODEOWNERS" not in record["attributes"]["path"]
        and "README" not in record["attributes"]["path"]
    ]
    readmes = [record for record in records if "README" in record["attributes"]["path"]]
    curr_blob = ""
    curr_tokens = 0
    curr_records_in_blob = []
    blob_records = []
    curr_blob_urls = []
    print(len(records))
    for record in filtered:
        print(record["title"])
        combo_tokens = record["attributes"]["num_tokens"] + curr_tokens
        if combo_tokens <= 5000:
            curr_tokens = combo_tokens
            curr_records_in_blob.append(record)
            curr_blob += f"\n\n{record['content']}"
            curr_blob_urls.append(record["url"])
        else:
            if curr_records_in_blob:
                if len(curr_records_in_blob) > 1:
                    blob_record = get_blob_record(
                        curr_records_in_blob,
                        curr_blob,
                        curr_tokens,
                        curr_blob_urls,
                        owner,
                        repo,
                    )
                    blob_records.append(blob_record)
                else:
                    if curr_records_in_blob[0]["attributes"]["num_tokens"] < 8000:
                        blob_records.append(curr_records_in_blob[0])
            curr_tokens = record["attributes"]["num_tokens"]
            curr_records_in_blob = [record]
            curr_blob = record["content"]
            curr_blob_urls = [record["url"]]
    if len(curr_records_in_blob) > 1:
        blob_record = get_blob_record(
            curr_records_in_blob, curr_blob, curr_tokens, curr_blob_urls, owner, repo
        )
        blob_records.append(blob_record)
    elif curr_records_in_blob:
        blob_records.append(curr_records_in_blob[0])
    blob_records.extend(readmes)
    return blob_records


def create_nb_records(owner, repo):
    root_url = f"https://api.github.com/repos/{owner}/{repo}/contents"
    files = search_directory_recursive(root_url, inclusion_filetypes=["ipynb"])
    records = []
    html_exporter = HTMLExporter(template_name="classic")
    html_converter = CustomHTML2Text()
    for file in files:
        response = file["content"]
        try:
            notebook = nbformat.reads(response, as_version=4)
        except:
            print(f"Error reading notebook {file['url']}")
            continue
        if (
            notebook.get("metadata") is not None
            and notebook["metadata"].get("widgets") is not None
        ):
            del notebook["metadata"]["widgets"]
        body, resources = html_exporter.from_notebook_node(notebook)
        content = filter_images(remove_indentation(html_converter.handle(body)))
        content_hash = get_content_hash(f"{file['url']}{content}")
        records.append(
            {
                "record_id": content_hash,
                "format": "md",
                "content": content,
                "url": file["url"],
                "title": find_readme_title(content).replace("¶", ""),
                "attributes": {
                    "hash_html": "",
                    "path": f"{owner}/{repo}/{file['path']}",
                    "hash_content": content_hash,
                    "num_tokens": get_num_tokens(content),
                    "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "record_type": "site",
                },
                "extra_attributes": {},
            }
        )
    graph_records = create_graph_records(records)
    return graph_records


def check_repo_existence(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.get(url)
    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        return True


# Note: GitHub's REST API considers every pull request an issue, but not every issue is a pull request.
# For this reason, "Issues" endpoints may return both issues and pull requests in the response.
# You can identify pull requests by the pull_request key.
# Be aware that the id of a pull request returned from "Issues" endpoints will be an issue id.
# To find out the pull request id, use the "List pull requests" endpoint.
def get_repository_issues(owner: str, repo: str, search_config):
    print("search_config", search_config)
    api_url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    for page in range(1, search_config["page_limit"] + 1):
        sleep(1)
        print(f"get_repository_issues page {page}")
        params = {
            "state": "all",
            "per_page": search_config["page_size"],
            "page": page,
            "since": "2023-01-01T00:00:00Z",
        }
        response = fetch_contents(api_url, params=params)

        for r in response:
            yield r

        if len(response) < search_config["page_size"]:
            return


def get_issue_comments_for_repository(owner: str, repo: str, search_config):
    api_url = f"https://api.github.com/repos/{owner}/{repo}/issues/comments"

    for page in range(1, search_config["page_limit"] + 1):
        sleep(1)
        params = {
            "per_page": search_config["page_size"],
            "page": page,
            "since": "2023-01-01T00:00:00Z",
        }
        try:
            req = fetch_contents(api_url, params=params)

            for r in req:
                if type(r) == dict:
                    yield r

            if len(req) < search_config["page_size"]:
                return
        except:
            print(f"failed to get comments for: {api_url}")
            return


def get_issues_and_comments_for_repository(
    owner: str, repo: str, search_config=PROD_SEARCH_CONFIG
):
    issues_and_pull_requests = list(get_repository_issues(owner, repo, search_config))
    issues = [x for x in issues_and_pull_requests if "pull_request" not in x]
    issues_map = {x["number"]: x for x in issues}

    for k, v in issues_map.items():
        v["_comments"] = []

    comments = list(get_issue_comments_for_repository(owner, repo, search_config))
    for comment in comments:
        if comment["issue_url"]:
            issue_number = int(comment["issue_url"].split("/")[-1])
            if issue_number in issues_map:
                issues_map[issue_number]["_comments"].append(comment)

    for k, v in issues_map.items():
        v["_comments"] = sorted(v["_comments"], key=lambda x: x["id"])

    yield from issues_map.values()


def get_issues_and_comments_for_config(owner, repo, search_config=PROD_SEARCH_CONFIG):
    issues_and_comments = get_issues_and_comments_for_repository(
        owner, repo, search_config
    )
    for issue_and_comment in issues_and_comments:
        # bind some stuff
        issue_and_comment["_owner"] = owner
        issue_and_comment["_repo"] = repo
        yield issue_and_comment


def get_issue_record_id(issue):
    comments_string = "".join([comment["body"] for comment in issue["_comments"]])
    issue_body = "" if issue["body"] is None else issue["body"]
    comments_string = "" if comments_string is None else comments_string
    return get_content_hash(issue_body + comments_string)


def create_issue_records(issues, owner, repo, apply_filters=True):
    records = []
    for issue in issues:
        if apply_filters:
            if issue["created_at"]:
                date = datetime.strptime(issue["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                if date < datetime(2023, 1, 1):
                    continue

        records.append(
            {
                "record_id": get_issue_record_id(issue),
                "format": "md",
                "content": issue["body"] if issue["body"] else "",
                "url": issue["html_url"],
                "title": issue["title"],
                "attributes": {
                    "state": issue["state"],
                    "reactions": json.dumps(issue.get("reactions", "{}")),
                    "comments": json.dumps(issue["_comments"]),
                    "created_at": issue["created_at"],
                    "owner": owner,
                    "repo": repo,
                    "author": issue["user"]["login"],
                    "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "record_type": "github_issues",
                },
                "extra_attributes": {},
            }
        )
    return records


def get_files_in_folder(folder_path, folder_name):
    """
    Iterate over all files in a folder.

    Args:
        folder_path (str): The path to the folder.

    Yields:
        str: The path of each file in the folder.
    """
    records = []
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            with open(file_path, "r") as f:
                print(file_path)
                content = f"# {file_name}\n"
                extension = file_path.split(".")[-1]
                language = language_map.get(extension, extension)

                if extension == ".png" or extension == ".ico":
                    continue

                if extension == ".md":
                    content += f
                elif extension == ".adoc":
                    content += run_adoc_helper(content).replace("```adoc", "")
                else:
                    content += f"```{language}\n{f}\n```"

                records.append(
                    {
                        "record_id": get_content_hash(content),
                        "format": "md",
                        "content": content,
                        "url": "https://stytch.com",
                        "title": file_path,
                        "attributes": {
                            "hash_html": "",
                            "path": file_path,
                            "hash_content": get_content_hash(content),
                            "num_tokens": get_num_tokens(content),
                            "index_version": datetime.now().strftime(
                                "%Y-%m-%dT%H:%M:%S"
                            ),
                            "record_type": "site",
                        },
                        "extra_attributes": {},
                    }
                )
    return records


def filter_images(markdown_string):
    # Regular expression to match markdown image syntax with base64 data
    pattern = r"!\[.*?\]\(data:image/(png|gif);base64,[A-Za-z0-9+/=\s]+\)"

    # Replace matched patterns with an empty string
    filtered_string = re.sub(pattern, "", markdown_string, flags=re.DOTALL)

    return filtered_string


def run_github_issues_pipeline(
    owner, repo, index_name, source_id, org_alias, project_id
):
    delete_mongodb_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )
    issues_and_comments = list(get_issues_and_comments_for_repository(owner, repo))

    records = create_issue_records(issues_and_comments, owner, repo)

    upload_batches(
        records,
        index_name,
        source_id,
        org_alias,
        project_id,
        indexing_engine=IndexingEngine.GITHUB_ISSUES,
    )


@task(name="github_issues_pipeline", log_prints=True)
def run_github_issues_pipeline_task(
    owner, repo, index_name, source_id, org_alias, project_id
):
    run_github_issues_pipeline(
        owner, repo, index_name, source_id, org_alias, project_id
    )


def run_github_readme_pipeline(
    owner, repo, index_name, source_id, org_alias, project_id
):
    delete_mongodb_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )
    records = create_readme_records(owner, repo)
    print(f"{len(records)} readme records created")
    upload_batches(
        records,
        f"{index_name}",
        source_id,
        org_alias,
        project_id,
        indexing_engine=IndexingEngine.GITHUB_READMES,
    )


@task(name="github_readme_task", log_prints=True)
def run_github_readme_pipeline_task(
    owner, repo, index_name, source_id, org_alias, project_id
):
    run_github_readme_pipeline(
        owner, repo, index_name, source_id, org_alias, project_id
    )


def run_github_nb_pipeline(owner, repo, index_name, source_id, org_alias, project_id):
    records = create_nb_records(owner, repo)
    upload_batches(
        records,
        index_name,
        source_id,
        org_alias,
        project_id,
        indexing_engine=IndexingEngine.GENERIC_DOCS,
        batch_size=10,
    )


from functools import partial
from concurrent.futures import ThreadPoolExecutor


def run_github_code_example_pipeline_test(
    owner,
    repo,
    index_name,
    source_id,
    org_alias,
    project_id,
    inclusion_filetypes=[],
    exclusion_filetypes=[],
    inclusion_filepaths=[],
    exclusion_filepaths=[],
    access_token=settings.GITHUB_TOKEN,
):
    # Start deletion asynchronously
    with ThreadPoolExecutor() as executor:
        delete_future = executor.submit(
            delete_mongodb_records_by_metadata,
            MongoRecordMetadataQueryInput(index_name=index_name),
        )

    # Handle ipynb files
    exclusion_filetypes = set(exclusion_filetypes + ["ipynb"])
    inclusion_filetypes = set(inclusion_filetypes)
    ipynb = "ipynb" in inclusion_filetypes
    if ipynb:
        inclusion_filetypes.remove("ipynb")

    # Fetch records concurrently
    with ThreadPoolExecutor() as executor:
        futures = []
        if ipynb:
            futures.append(executor.submit(create_nb_records, owner, repo))
        if not ipynb or inclusion_filetypes:
            futures.append(
                executor.submit(
                    create_entire_repo_records,
                    owner,
                    repo,
                    inclusion_filetypes=list(inclusion_filetypes),
                    exclusion_filetypes=list(exclusion_filetypes),
                    inclusion_filepaths=inclusion_filepaths,
                    exclusion_filepaths=exclusion_filepaths,
                    access_token=access_token,
                )
            )

        # Wait for all futures to complete
        results = [future.result() for future in futures]

    # Process results
    nb_records = results[0] if ipynb else []
    records = results[-1] if not ipynb or inclusion_filetypes else []

    # Get repo blob concurrently if needed
    if records:
        with ThreadPoolExecutor() as executor:
            blob_future = executor.submit(get_repo_blob, records, owner, repo)
            delete_future.result()  # Ensure deletion is complete
            blob_records = blob_future.result()
            blob_records.extend(nb_records)
    else:
        delete_future.result()  # Ensure deletion is complete
        blob_records = nb_records

    # Upload batches
    upload_batches(blob_records, index_name, source_id, org_alias, project_id)


def run_github_code_example_pipeline(
    owner,
    repo,
    index_name,
    source_id,
    org_alias,
    project_id,
    inclusion_filetypes=[],
    exclusion_filetypes=[],
    inclusion_filepaths=[],
    exclusion_filepaths=[],
    access_token=settings.GITHUB_TOKEN,
):
    delete_mongodb_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )
    exclusion_filetypes.append("ipynb")
    nb_records = []
    ipynb = False
    if "ipynb" in inclusion_filetypes:
        ipynb = True
        inclusion_filetypes.remove("ipynb")
        nb_records = create_nb_records(owner, repo)

    if ipynb and not inclusion_filetypes:
        upload_batches(nb_records, index_name, source_id, org_alias, project_id)
    else:
        records = create_entire_repo_records(
            owner,
            repo,
            inclusion_filetypes=inclusion_filetypes,
            exclusion_filetypes=exclusion_filetypes,
            inclusion_filepaths=inclusion_filepaths,
            exclusion_filepaths=exclusion_filepaths,
            access_token=access_token,
        )
        if "devcode-git" != owner:
            blob_records = get_repo_blob(records, owner, repo)
            blob_records.extend(nb_records)
            upload_batches(blob_records, index_name, source_id, org_alias, project_id)
        else:
            records.extend(nb_records)
            upload_batches(records, index_name, source_id, org_alias, project_id)


@task(name="github_example_pipeline", log_prints=True)
def run_github_code_example_pipeline_task(
    owner,
    repo,
    index_name,
    source_id,
    org_alias,
    project_id,
    inclusion_filetypes=[],
    exclusion_filetypes=[],
    inclusion_filepaths=[],
    exclusion_filepaths=[],
    access_token=settings.GITHUB_TOKEN,
):
    run_github_code_example_pipeline(
        owner,
        repo,
        index_name,
        source_id,
        org_alias,
        project_id,
        inclusion_filetypes=inclusion_filetypes,
        exclusion_filetypes=exclusion_filetypes,
        inclusion_filepaths=inclusion_filepaths,
        exclusion_filepaths=exclusion_filepaths,
        access_token=settings.GITHUB_TOKEN,
    )


def get_most_recent_commit_time(owner, repo):
    response = fetch_contents("https://api.github.com/repos/PostHog/posthog-js/commits")
    date_str = response[0]["commit"]["author"]["date"]
    # Replace 'Z' with '+00:00' to indicate UTC
    iso_str = date_str.replace("Z", "+00:00")

    # Convert to datetime object
    dt = datetime.fromisoformat(iso_str)

    return dt


# def poll_github_repos():
#     orgs = get_active_orgs()
#     for org, status in orgs.items():
#         if org in ORGS_WITH_GITHUB:
#             sources = get_sources_for_org(org)
#             for source in sources:
#                 if (
#                     source["__typename"] == "GitHubSource"
#                     and source["includeREADMEs"]
#                     and source["includeSourceCode"]
#                 ):
#                     owner = source["owner"]
#                     repo = source["repo"]
#                     most_recent_sync_time = get_most_recent_sync_time(source["id"], org)
#                     most_recent_commit_time = get_most_recent_commit_time(owner, repo)
#                     if not most_recent_sync_time or (
#                         most_recent_commit_time > most_recent_sync_time
#                     ):
#                         ping_github_ingester(org, source["id"], [])


def make_request(query):
    headers = {
        "Authorization": f"bearer {settings.GITHUB_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        "https://api.github.com/graphql",
        json={"query": query},
        headers=headers,
    )

    data = response.json()
    return json.dumps(data, indent=4)


def get_github_discussions(owner, repo, cursor=None):
    discussion_args = "first:100" if not cursor else f'first:100, after:"{cursor}"'
    query = f"""
    {{
        repository(owner: \"{owner}\", name: \"{repo}\"){{
            discussions({discussion_args}){{
                totalCount
                nodes {{
                    url
                    number
                    body
                    createdAt
                    lastEditedAt
                    title
                    id
                    author {{
                        login
                    }}
                    comments(first: 10){{
                        totalCount
                        nodes {{
                            body
                            createdAt
                            lastEditedAt
                            isAnswer
                            author {{
                                login
                            }}
                            replies(first: 100){{ 
                                nodes {{
                                    body
                                    createdAt
                                    lastEditedAt
                                    isAnswer
                                    author{{
                                        login
                                    }}
                                }}
                            }}
                        }}
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                    }}
                    category{{
                        name
                    }}
                }}
                
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }} 
        }}
    }}
    """
    return make_request(query)


def get_all_discussions(owner, repo):
    responses = []
    curr_response = get_curr_response(owner, repo)
    curr_response["nodes"] = fill_discussions(owner, repo, curr_response["nodes"])
    responses.append(curr_response)
    count = 1
    while curr_response["pageInfo"]["hasNextPage"]:
        curr_response = get_curr_response(
            owner, repo, cursor=curr_response["pageInfo"]["endCursor"]
        )
        if not curr_response:
            break
        curr_response["nodes"] = fill_discussions(owner, repo, curr_response["nodes"])
        count += 1
        print(f"getting discussion page: {count}")
        responses.append(curr_response)
    return responses


def get_curr_response(owner, repo, cursor=None):
    try:
        response = get_github_discussions(owner, repo, cursor=cursor)
        curr_response = json.loads(response)["data"]["repository"]["discussions"]
        return curr_response
    except KeyError:
        print(response)
        return None


def fill_discussions(owner, repo, discussion_list):
    for discussion in discussion_list:
        discussion["comments"]["nodes"].extend(
            get_additional_comments(owner, repo, discussion)
        )
    return discussion_list


def get_additional_comments(owner, repo, discussion):
    discussion_number = discussion["number"]
    cursor = (
        discussion["comments"]["pageInfo"]["endCursor"]
        if discussion["comments"]["pageInfo"]["hasNextPage"]
        else None
    )

    additional_comments = []
    while cursor:
        query = f"""
        {{
            repository(owner: \"{owner}\", name: \"{repo}\"){{
                discussion(number:{discussion_number}){{
                    comments(first: 10, after:\"{cursor}\"){{
                        totalCount
                        nodes {{
                            body
                            createdAt
                            isAnswer
                            lastEditedAt
                            author {{
                                login
                            }}
                            replies(first: 50){{ 
                                nodes {{
                                    body
                                    createdAt
                                    isAnswer
                                    lastEditedAt
                                    author {{
                                        login
                                    }}
                                }}
                            }}
                        }}
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                    }}
                }} 
            }}
        }}
        """
        try:
            response = json.loads(make_request(query))
            response = response["data"]["repository"]["discussion"]
        except KeyError:
            print(response)
            raise

        cursor = (
            response["comments"]["pageInfo"]["endCursor"]
            if response["comments"]["pageInfo"]["hasNextPage"]
            else None
        )
        additional_comments.append(response["comments"]["nodes"])
    print(f"found {len(additional_comments)} additional comments")
    return additional_comments


def format_comments(comments, is_reply=False):
    comment_records = []
    filtered_comments = []
    for comment in comments:
        if type(comment) == list:
            filtered_comments.extend(comment)
        else:
            filtered_comments.append(comment)
    for comment in filtered_comments:
        try:
            record = {
                "body": comment["body"],
                "timestamp": comment["createdAt"],
                "author": comment["author"]["login"] if comment["author"] else "",
                "isAnswer": comment["isAnswer"],
            }
            if not is_reply:
                record["replies"] = format_comments(
                    comment["replies"]["nodes"], is_reply=True
                )
        except:
            print(comment)
            print(comments)
            raise
        comment_records.append(record)
    return comment_records


def get_comment_id_content(formatted_comments):
    output = ""
    for comment in formatted_comments:
        output = output + comment["body"] + str(comment["isAnswer"])
        if "replies" in comment and comment["replies"]:
            output += get_comment_id_content(comment["replies"])
    return output


def create_discussion_records(owner, repo, apply_filters=True):
    discussion_groups = get_all_discussions(owner, repo)
    records = []
    for discussion_group in discussion_groups:
        for discussion in discussion_group["nodes"]:
            if apply_filters:
                if discussion["lastEditedAt"]:
                    date = datetime.strptime(
                        discussion["lastEditedAt"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                else:
                    date = datetime.strptime(
                        discussion["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                if date < datetime(2023, 1, 1):
                    continue
                if len(discussion["comments"]["nodes"]) == 0:
                    continue
            comments = format_comments(discussion["comments"]["nodes"])
            record_id = get_content_hash(
                discussion["title"]
                + discussion["body"]
                + get_comment_id_content(comments)
            )
            records.append(
                {
                    "record_id": record_id,
                    "title": discussion["title"],
                    "content": discussion["title"],
                    "format": "txt",
                    "url": discussion["url"],
                    "attributes": {
                        "author": (
                            discussion["author"]["login"]
                            if discussion["author"]
                            else "unkown user"
                        ),
                        "timestamp": discussion["createdAt"],
                        "body": discussion["body"],
                        "category": discussion["category"]["name"],
                        "discussion_id": discussion["id"],
                        "discussion_number": discussion["number"],
                        "comments": json.dumps(comments),
                        "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    },
                    "extra_attributes": {},
                }
            )

    return records


def get_discussion_diffs(old_records, new_records):
    old_records = {record["record_id"]: record for record in old_records}
    new_records = {record["record_id"]: record for record in new_records}
    old_ids = set(old_records.keys())
    new_ids = set(new_records.keys())
    new = new_ids - old_ids
    deleted = old_ids - new_ids
    modified = set()
    for id in old_ids.intersection(new_ids):
        old_discussion = format_github_discussions(old_records[id])
        new_discussion = format_github_discussions(new_records[id])
        if old_discussion != new_discussion:
            modified.add(id)
    new = [new_records[id] for id in new]
    modified = [new_records[id] for id in modified]

    return (
        new,
        deleted,
        modified,
    )  # new and modified contain the full records while deleted contains only the ids


def create_and_compare_discussion_records(owner, repo, index_name):
    new_discussion_records = create_discussion_records(
        owner,
        repo,
    )
    old_discussion_records = get_mongo_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )
    discussion_diffs = get_discussion_diffs(
        old_discussion_records, new_discussion_records
    )

    return discussion_diffs


def upload_discussion_records(records, index_name, source_id, org_alias, project_id):
    print("initialized index")
    upload_batches(
        records,
        index_name,
        source_id,
        org_alias,
        project_id,
        indexing_engine=IndexingEngine.GITHUB_DISCUSSIONS,
    )
    print("uploaded batches")


def run_discussion_pipeline(
    owner, repo, index_name, source_id, org_alias, project_id, apply_filters=True
):
    delete_mongodb_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )
    records = create_discussion_records(owner, repo, apply_filters=apply_filters)
    print(f"{len(records)} discussion records created")
    upload_discussion_records(records, index_name, source_id, org_alias, project_id)


@task(name="run_discussion_pipeline", log_prints=True)
def run_discussion_pipeline_task(
    owner, repo, index_name, source_id, org_alias, project_id, apply_filters=True
):
    run_discussion_pipeline(
        owner,
        repo,
        index_name,
        source_id,
        org_alias,
        project_id,
        apply_filters=apply_filters,
    )


def format_date(timestamp_str):
    # Parse the timestamp string
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")

    # Format the datetime object as required
    formatted_date = timestamp.strftime("%B %d, %Y at %H:%M")

    return formatted_date


def format_github_discussions(record):
    formatted_discussion = ""
    formatted_discussion += f"Title: {record['content']}\n"
    formatted_discussion += f"Category: {record['attributes']['category']}\n"
    formatted_discussion += f"Author: {record['attributes']['author']}\n"
    formatted_discussion += (
        f"Timestamp: {format_date(record['attributes']['timestamp'])}\n"
    )
    formatted_discussion += f"Body: '''{record['attributes']['body']}'''\n"
    formatted_discussion += f"Comments:\n"
    comments = json.loads(record["attributes"]["comments"])
    for i, comment in enumerate(comments):
        formatted_discussion += f"    Comment #{i+1} from {comment['author']} on {format_date(comment['timestamp'])}: '''{comment['body']}'''\n"
        formatted_discussion += f"    Replies:\n"
        for j, reply in enumerate(comment["replies"]):
            formatted_discussion += f"        Reply #{j+1} to comment #{i+1} from {['author']} on {format_date(reply['timestamp'])}: '''{reply['body']}''' \n"
    return formatted_discussion


def sync_github(org_alias, source_id, maintenance=True):
    org_configs = get_org_configs(org_alias, map_source_ids=True)
    configs = org_configs[source_id]
    sync_jobs = get_source_sync_job_mapping(
        org_alias, status_requirement="PROCESSING", status_message_requirement="queued"
    )
    if source_id not in sync_jobs:
        return
    else:
        sync_jobs = sync_jobs[source_id]

    initial_project_id = ""
    for config in configs:
        input_config: CreateIndexInput = config["input"]
        github_info: GithubInfo = input_config.github_info
        github_config = {
            "owner": github_info.owner,
            "repo": github_info.repo,
            "index_name": config["base_index_name"],
            "source_id": input_config.source_id,
            "org_alias": input_config.org_alias,
            "project_id": input_config.project_ids[0],
        }
        print(github_info)
        try:
            if github_info.includeREADMEs:
                print(github_config["index_name"])
                run_github_readme_pipeline_task(**github_config)

            elif github_info.includeDiscussions:
                run_discussion_pipeline_task(**github_config)
            elif github_info.includeIssues:
                run_github_issues_pipeline_task(**github_config)
            elif github_info.includeSourceCode:
                inclusion_patterns = [
                    include_pattern.pattern
                    for include_pattern in input_config.crawler_matching_patterns.include_patterns
                ]
                exclusion_patterns = [
                    exclude_pattern.pattern
                    for exclude_pattern in input_config.crawler_matching_patterns.exclude_patterns
                ]

                if org_alias == "ably":
                    inclusion_filepaths = inclusion_patterns
                    exclusion_filepaths = exclusion_patterns
                    inclusion_filetypes = []
                    exclusion_filetypes = []
                else:
                    inclusion_filetypes = inclusion_patterns
                    exclusion_filetypes = exclusion_patterns
                    inclusion_filepaths = []
                    exclusion_filepaths = []

                run_github_code_example_pipeline_task(
                    **github_config,
                    inclusion_filepaths=inclusion_filepaths,
                    exclusion_filepaths=exclusion_filepaths,
                    inclusion_filetypes=inclusion_filetypes,
                    exclusion_filetypes=exclusion_filetypes,
                )
        except Exception as e:
            print(f"Failed to scrape GitHub with error: {str(e)}")
            fail_indexing_jobs(org_alias, sync_jobs, "Failed to scrape github")
            raise (e)

    update_all_indexing_jobs(
        org_alias, sync_jobs, status="PROCESSING", status_message="ingested"
    )

    if maintenance:
        trigger_auto_index_deploy(org_alias, configs)
    else:
        trigger_manual_index_deploy(org_alias, configs)


@flow(name="sync_github_source_flow", log_prints=True)
def sync_github_source_flow(org_alias, source_id):
    sync_github(org_alias, source_id, maintenance=False)


@flow(name="queue_qithub_flow", log_prints=True)
def queue_github_flow():
    orgs = get_active_orgs()
    for org, status in orgs.items():
        if org in {"neon", "singlestore", "vercel", "zilliz"} or status not in {
            "CUSTOMER",
            "PROSPECT",
        }:
            continue
        sources = get_sources_for_org(org)
        github_sources = []
        for source in sources:
            if source["__typename"] == "GitHubSource":
                github_sources.append(source["id"])

        for source_id in github_sources:
            print(f"creating github jobs for {org}, {source_id}")
            create_source_sync_and_indexing_jobs(
                org, source_id, status="PROCESSING", status_message="queued"
            )

            sync_github(org, source_id, maintenance=True)

        if github_sources:
            time.sleep(300)


@task(name="trigger-indexing-task")
def trigger_auto_index_deploy(org, configs):
    index_mapping = create_index_mapping(org, configs)
    body = {"index_mapping": index_mapping, "org_alias": org}
    deployment_run = run_deployment(
        name="index_sync_job_flow/gcp-auto-indexing-deploy", parameters={"input": body}
    )
    return deployment_run


@task(name="trigger-manual-indexing-task")
def trigger_manual_index_deploy(org, configs):
    index_mapping = create_index_mapping(org, configs)
    body = {"index_mapping": index_mapping, "org_alias": org}
    deployment_run = run_deployment(
        name="index_sync_job_flow/gcp-indexing-deploy", parameters={"input": body}
    )


@flow(name="run_worldline_sync", log_prints=True)
def run_worldline_sync():
    github_config = {
        "owner": "devcode-git",
        "repo": "PaymentIQ-Documentation-Portal",
        "index_name": "inkeep_worldline_github-examples-devcode-git-PaymentIQ-Documentation-Portal",
        "source_id": "cm1z77kj601h8rcy242nimyxc",
        "org_alias": "worldline",
        "project_id": "cm1t1mqg3011s6sj8pfact6sw",
    }
    run_github_code_example_pipeline(
        **github_config,
        inclusion_filetypes=["docs/.*"],
        exclusion_filetypes=[
            "docs/release_notes/.*",
            "docs/configuration_and_administration/provider_integration_manuals/_template.md",
        ],
    )
    create_source_sync_and_indexing_jobs(
        "worldline",
        "cm1z77kj601h8rcy242nimyxc",
        status="PROCESSING",
        status_message="ingested",
    )
    create_source_sync_and_indexing_jobs(
        "worldline",
        "cm3elvkhd004o5o7i1gqfggoo",
        status="PROCESSING",
        status_message="ingested",
    )
    configs = get_org_configs("worldline", map_source_ids=True)
    trigger_auto_index_deploy("worldline", configs["cm1z77kj601h8rcy242nimyxc"])
    trigger_auto_index_deploy("worldline", configs["cm3elvkhd004o5o7i1gqfggoo"])


if __name__ == "__main__":
    # github_config = {
    #     "owner": "devcode-git",
    #     "repo": "PaymentIQ-Documentation-Portal",
    #     "index_name": "inkeep_worldline_github-examples-devcode-git-PaymentIQ-Documentation-Portal",
    #     "source_id": "cm1z77kj601h8rcy242nimyxc",
    #     "org_alias": "worldline",
    #     "project_id": "cm1t1mqg3011s6sj8pfact6sw",
    # }
    github_config = {
        "owner": "gravitational",
        "repo": "teleport",
        "index_name": "inkeep_teleport_github-readmes-gravitational-teleport",
        "source_id": "clnvxtp0o000ms6013imyw5kq",
        "org_alias": "teleport",
        "project_id": "clsz3nbvo0002m3ej1kxs87vm",
    }
    run_github_readme_pipeline(**github_config)
