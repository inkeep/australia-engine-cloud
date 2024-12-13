import typing as T
from datetime import datetime
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from config import settings


def get_inkeep_graphql_client(organization_id=None, organization_alias=None):
    if organization_id:
        transport = RequestsHTTPTransport(
            url=settings.INKEEP_MANAGEMENT_GRAPHQL_API_URL,
            headers={
                "Imitated-Organization-Id": organization_id,
                "Authorization": f"Bearer {settings.INKEEP_MANAGEMENT_GRAPHQL_API_KEY}",
            },
        )
    elif organization_alias:
        transport = RequestsHTTPTransport(
            url=settings.INKEEP_MANAGEMENT_GRAPHQL_API_URL,
            headers={
                "Imitated-Organization-Alias": organization_alias,
                "Authorization": f"Bearer {settings.INKEEP_MANAGEMENT_GRAPHQL_API_KEY}",
            },
        )
    else:
        # Select your transport with a defined url endpoint
        transport = RequestsHTTPTransport(
            url=settings.INKEEP_MANAGEMENT_GRAPHQL_API_URL,
            headers={
                "Authorization": f"Bearer {settings.INKEEP_MANAGEMENT_GRAPHQL_API_KEY}",
            },
        )
    # Create a GraphQL client using the defined transport
    client = Client(transport=transport, execute_timeout=120)
    return client


def create_source_sync_job(org, source_id):
    client = get_inkeep_graphql_client(organization_alias=org)
    mutation = gql(
        """
        mutation createSourceSyncJob($sourceId: ID!){
            createSourceSyncJob(input: {
                sourceId: $sourceId
            }){
                job{
                    id
                }
            }
        }
        """
    )
    variables = {"sourceId": source_id}
    result = client.execute(mutation, variable_values=variables)
    return result["createSourceSyncJob"]["job"]["id"]


def get_source(source_id, org_alias="", org_id=""):
    if org_alias:
        client = get_inkeep_graphql_client(organization_alias=org_alias)
    if org_id:
        client = get_inkeep_graphql_client(organization_id=org_id)
    query = gql(
        """
        query($sourceId: ID!){
            source(sourceId: $sourceId){
                id
                __typename
                displayName
                indexes {
                    id
                    indexingEngine
                }
                syncJobHistory {
                    endTime
                    id
                    indexingJobs {
                        endTime
                        id
                        startTime
                        status
                        statusMessage
                    }
                    startTime
                    status
                    statusMessage
                    type
                }
                urlMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                titleMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                ... on GeneralWebSource {
                    isAutoRefreshEnabled
                    ingestUrls
                    crawlerStartUrls
                    crawlerSitemapUrls
                    isDocumentation
                    filterXPaths
                    dateXPaths
                }
                ... on GitHubSource {
                    isAutoRefreshEnabled
                    includeDiscussions
                    includePullRequests
                    includeREADMEs
                    includeReleaseNotes
                    includeIssues
                    includeSourceCode
                    owner
                    repo
                    url
                }
                ... on ZendeskSource {
                    isAutoRefreshEnabled
                    helpCenterUrl
                }
                ... on DiscordSource {
                    isAutoRefreshEnabled
                    channelIds
                    serverId
                    teamMemberRoles
                }
                ... on DocusaurusSource {
                    isAutoRefreshEnabled
                    url
                }
                ... on GitbookSource{
                    isAutoRefreshEnabled
                    url              
                }
                ... on ReadmeSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on RedoclySource{
                    isAutoRefreshEnabled
                    url
                }
                ... on DiscordSource{
                    isAutoRefreshEnabled
                    serverId
                    channelIds
                    teamMemberRoles
                }
                ... on DiscourseSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on ApiSpecSource{
                    isAutoRefreshEnabled
                    citationUrl
                    contentUrl
                }
                ... on SlackSource{
                    isAutoRefreshEnabled
                    workspaceId
                    channelIds
                }
                ... on NotionSource{
                    connectionId
                    isAutoRefreshEnabled
                }
                ... on ZendeskSupportTicketsSource {
                    isAutoRefreshEnabled
                    connectionId
                }
            }
        }
    """
    )
    variables = {"sourceId": source_id}
    result = client.execute(query, variable_values=variables)
    return result["source"]


def create_indexing_job(
    org_alias, index_id, source_sync_id, status="QUEUED", status_message=""
):
    client = get_inkeep_graphql_client(organization_alias=org_alias)
    start_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    mutation = gql(
        """
        mutation($indexId: ID!, $startTime: DateTime!, $status: IndexingJobStatus!, $sourceSyncId: ID!, $statusMessage: String!){
            createIndexingJob(input: {
                indexId: $indexId
                sourceSyncJobId: $sourceSyncId
                job:{
                    startTime: $startTime
                    status: $status
                    statusMessage: $statusMessage
                }
            }){
                job{
                    id
                }
            }
        }
    """
    )
    variables = {
        "indexId": index_id,
        "startTime": start_time,
        "status": status,
        "statusMessage": status_message,
        "sourceSyncId": source_sync_id,
    }
    result = client.execute(mutation, variable_values=variables)
    return result["createIndexingJob"]["job"]["id"]


def create_source_sync_and_indexing_jobs(
    org: str, source_id: str, status="PROCESSING", status_message="local"
):
    ss_id = create_source_sync_job(org, source_id)
    source = get_source(source_id, org)
    index_ids = [index["id"] for index in source["indexes"]]
    for index_id in index_ids:
        create_indexing_job(org, index_id, ss_id, status, status_message)


def get_sources_for_org(org_alias):
    client = get_inkeep_graphql_client(organization_alias=org_alias)
    query = gql(
        """
        query{
            allProjects {
                id
                sources {
                id
                displayName
                __typename
                indexes {
                    id
                    indexingEngine
                }
                urlMatchingPatterns {
                        excludePatterns {
                            matchType
                            pattern
                        }
                        includePatterns {
                            matchType
                            pattern
                        }
                    }
                titleMatchingPatterns {
                        excludePatterns {
                            matchType
                            pattern
                        }
                        includePatterns {
                            matchType
                            pattern
                        }
                    }
                syncJobHistory {
                        endTime
                        id
                        indexingJobs {
                            endTime
                            id
                            startTime
                            status
                            statusMessage
                        }
                        startTime
                        status
                        statusMessage
                        type
                    }                
                    ... on GeneralWebSource {
                    isAutoRefreshEnabled
                    ingestUrls
                    crawlerStartUrls
                    crawlerSitemapUrls
                    isDocumentation
                    filterXPaths
                    dateXPaths
                }
                ... on GitHubSource {
                    isAutoRefreshEnabled
                    includeDiscussions
                    includePullRequests
                    includeREADMEs
                    includeReleaseNotes
                    includeIssues
                    includeSourceCode
                    owner
                    repo
                    url
                }
                ... on ZendeskSource {
                    isAutoRefreshEnabled
                    helpCenterUrl
                    __typename
                }
                ... on DiscordSource {
                    isAutoRefreshEnabled
                    channelIds
                    serverId
                    teamMemberRoles
                }
                ... on DocusaurusSource {
                    isAutoRefreshEnabled
                    url
                }
                ... on GitbookSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on ReadmeSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on RedoclySource{
                    isAutoRefreshEnabled
                    url
                }
                ... on DiscordSource{
                    isAutoRefreshEnabled
                    serverId
                    channelIds
                    teamMemberRoles
                }
                ... on DiscourseSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on ApiSpecSource{
                    isAutoRefreshEnabled
                    citationUrl
                    contentUrl
                }
                ... on SlackSource{
                    isAutoRefreshEnabled
                    workspaceId
                    channelIds
                }  
                ... on NotionSource{
                    connectionId
                    isAutoRefreshEnabled
                }
                ... on ZendeskSupportTicketsSource {
                    isAutoRefreshEnabled
                    connectionId
                }
            }
        }
    }
    """
    )
    result = client.execute(query)
    source_list = []
    for sources in result["allProjects"]:
        source_list.extend(sources["sources"])
    return source_list


def get_active_orgs():
    client = get_inkeep_graphql_client()
    query = gql(
        """
        query{
            allOrganizationsMetadata{
                status
                alias
            }
        }
    """
    )
    result = client.execute(query)
    orgs = {}
    whitelist = {
        "zitadel",
        "weaviate",
        "tamagui",
        "tailscale",
        "cypressio",
        "remitano",
        "torida",
    }
    for res in result["allOrganizationsMetadata"]:
        if (
            res["status"] == "CUSTOMER"
            or res["status"] == "PROSPECT"
            or res["status"] == "DEMO"
            or res["status"] == "SELF_SERVICE_DEMO"
            or res["alias"] in whitelist
        ):
            orgs[res["alias"]] = res["status"]
    return orgs


def get_source_sync_job_mapping(
    org_alias, status_requirement="QUEUED", status_message_requirement=""
) -> T.Dict[str, T.List[T.Dict[str, T.Any]]]:
    sources = get_sources_for_org(org_alias)
    source_sync_jobs = {}
    for source in sources:
        if "syncJobHistory" in source and source["syncJobHistory"]:
            jobs = sort_source_sync_jobs(
                source["syncJobHistory"],
                status_requirement=status_requirement,
                status_message_requirement=status_message_requirement,
            )
            if jobs:
                source_sync_jobs[source["id"]] = jobs
    return source_sync_jobs


def get_most_recent_sync_time(source_id, org_alias):
    sync_jobs = get_source_sync_job_mapping(org_alias, status_requirement="SUCCESSFUL")
    if source_id in sync_jobs:
        sorted_jobs = sort_source_sync_jobs(
            sync_jobs[source_id], status_requirement="SUCCESSFUL"
        )
        endtime = sorted_jobs[0]["endTime"]

        # Replace 'Z' with '+00:00' to indicate UTC
        iso_str = "2024-09-01T22:17:43.000Z".replace("Z", "+00:00")

        # Convert to datetime object
        dt = datetime.fromisoformat(iso_str)

        return dt
    return None


def sort_source_sync_jobs(
    sync_jobs, status_requirement="QUEUED", status_message_requirement=""
):
    sorted_jobs = sorted(
        sync_jobs,
        key=lambda x: datetime.strptime(x["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        reverse=True,
    )
    filtered_jobs = []
    for job in sorted_jobs:
        if status_requirement == "QUEUED" and job["status"] == "QUEUED":
            filtered_jobs.append(job)
        elif status_requirement == job["status"] and not status_message_requirement:
            filtered_jobs.append(job)
        elif (
            status_requirement == job["status"]
            and status_message_requirement == job["statusMessage"]
        ):
            filtered_jobs.append(job)

    full_scrape_queued = False
    for job in filtered_jobs:
        if job["type"] == "FULL":
            full_scrape_queued = True
            break

    if full_scrape_queued:
        filtered_jobs[0]["type"] = "FULL"

    if filtered_jobs:
        return filtered_jobs
    else:
        return []


def get_sources_for_project(org_alias, project_id):
    client = get_inkeep_graphql_client(organization_alias=org_alias)

    query = gql(
        """
        query($projectId: ID!){
            project(projectId: $projectId){
                sources {
                    id
                    __typename
                    displayName
                    indexes {
                        id
                        indexingEngine
                    }
                urlMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                titleMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                ... on GeneralWebSource {
                    isAutoRefreshEnabled
                    ingestUrls
                    crawlerStartUrls
                    crawlerSitemapUrls
                    isDocumentation
                    filterXPaths
                    dateXPaths
                    syncJobHistory {
                        endTime
                        id
                        indexingJobs {
                            endTime
                            id
                            startTime
                            status
                            statusMessage
                        }
                        startTime
                        status
                        statusMessage
                        type
                    }
                }
                ... on GitHubSource {
                    isAutoRefreshEnabled
                    includeDiscussions
                    includePullRequests
                    includeREADMEs
                    includeReleaseNotes
                    includeIssues
                    includeSourceCode
                    owner
                    repo
                }
                ... on ZendeskSource {
                    isAutoRefreshEnabled
                    helpCenterUrl
                }
                ... on DiscordSource {
                    isAutoRefreshEnabled
                    channelIds
                    serverId
                    teamMemberRoles
                }
                ... on DocusaurusSource {
                    isAutoRefreshEnabled
                    url
                }
                ... on GitbookSource{
                    isAutoRefreshEnabled
                    url                
                }
                ... on ReadmeSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on RedoclySource{
                    isAutoRefreshEnabled
                    url
                }
                ... on DiscordSource{
                    isAutoRefreshEnabled
                    serverId
                    channelIds
                    teamMemberRoles
                }
                ... on DiscourseSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on ApiSpecSource{
                    isAutoRefreshEnabled
                    citationUrl
                    contentUrl
                }
                ... on SlackSource{
                    isAutoRefreshEnabled
                    workspaceId
                    channelIds
                }       
                ... on NotionSource{
                    connectionId
                    isAutoRefreshEnabled
                }
                ... on ZendeskSupportTicketsSource {
                    isAutoRefreshEnabled
                    connectionId
                }
            }   
        }
    }
    """
    )

    variables = {"projectId": project_id}
    result = client.execute(query, variable_values=variables)
    return result["project"]["sources"]


def get_project_source_map_for_org(org_alias):
    project_source_map = {}
    client = get_inkeep_graphql_client(organization_alias=org_alias)
    query = gql(
        """
        query{
            allProjects {
                id
                sources {
                __typename
                id
                indexes {
                    id
                    indexingEngine
                }
                urlMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                titleMatchingPatterns {
                    excludePatterns {
                        matchType
                        pattern
                    }
                    includePatterns {
                        matchType
                        pattern
                    }
                }
                ... on GeneralWebSource {
                    isAutoRefreshEnabled
                    ingestUrls
                    crawlerStartUrls
                    crawlerSitemapUrls
                    isDocumentation
                    filterXPaths
                    dateXPaths
                    syncJobHistory {
                        endTime
                        id
                        indexingJobs {
                            endTime
                            id
                            startTime
                            status
                            statusMessage
                        }
                        startTime
                        status
                        statusMessage
                        type
                    }
                }
                ... on GitHubSource {
                    isAutoRefreshEnabled
                    includeDiscussions
                    includePullRequests
                    includeREADMEs
                    includeReleaseNotes
                    includeIssues
                    includeSourceCode
                    owner
                    repo
                }
                ... on ZendeskSource {
                    isAutoRefreshEnabled
                    helpCenterUrl
                }
                ... on DiscordSource {
                    isAutoRefreshEnabled
                    channelIds
                    serverId
                    teamMemberRoles
                }
                ... on DocusaurusSource {
                    isAutoRefreshEnabled
                    displayName
                    url
                }
                ... on GitbookSource{
                    isAutoRefreshEnabled
                    displayName
                    url
                }
                ... on ReadmeSource{
                    isAutoRefreshEnabled
                    displayName
                    url
                }
                ... on RedoclySource{
                    isAutoRefreshEnabled
                    displayName
                    url
                }
                ... on DiscordSource{
                    isAutoRefreshEnabled
                    serverId
                    channelIds
                    teamMemberRoles
                }
                ... on DiscourseSource{
                    isAutoRefreshEnabled
                    url
                }
                ... on ApiSpecSource{
                    isAutoRefreshEnabled
                    citationUrl
                    contentUrl
                }
                ... on SlackSource{
                    isAutoRefreshEnabled
                    workspaceId
                    channelIds
                }       
                ... on NotionSource{
                    isAutoRefreshEnabled
                    connectionId
                }
                ... on ZendeskSupportTicketsSource {
                    isAutoRefreshEnabled
                    connectionId
                }
            }
        }
    }
    """
    )
    result = client.execute(query)
    for project in result["allProjects"]:
        project_source_map[project["id"]] = project["sources"]
    return project_source_map


def update_indexing_job(
    org_alias,
    job_id,
    status,
    end_time=datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    status_message="",
):
    client = get_inkeep_graphql_client(organization_alias=org_alias)
    mutation = gql(
        """
        mutation($indexingJobId: ID!, $status: IndexingJobStatus!, $endTime: DateTime, $statusMessage: String){
            updateIndexingJob(input: {
                indexingJobId: $indexingJobId
                status: $status
                endTime: $endTime
                statusMessage: $statusMessage
            }){
                success
                job{
                    id
                    status
                    statusMessage
                }
            }
        }
    """
    )
    variables = {
        "indexingJobId": job_id,
        "status": status,
        "endTime": end_time,
        "statusMessage": status_message,
    }
    result = client.execute(mutation, variable_values=variables)
    return result["updateIndexingJob"]


def fail_indexing_jobs(org, sync_jobs, message):
    print("failing all indexing jobs")
    for sync_job in sync_jobs:
        for index in sync_job["indexingJobs"]:
            print(f"failing job {index['id']}")
            print(
                update_indexing_job(org, index["id"], "FAILED", status_message=message)
            )


def update_all_indexing_jobs(org_alias, sync_jobs, status, status_message):
    for job in sync_jobs:
        for indexing_job in job["indexingJobs"]:
            update_indexing_job(
                org_alias,
                indexing_job["id"],
                status=status,
                status_message=status_message,
            )
            update_indexing_job(
                org_alias,
                indexing_job["id"],
                status=status,
                status_message=status_message,
            )


def get_all_sources_of_type(
    typename, status_requirements={"CUSTOMER", "PROSPECT", "SELF_SERVICE_DEMO"}
):
    orgs = get_active_orgs()
    results = {}
    for org, status in orgs.items():
        if org in {"mintlify", "mintlifydev"}:
            continue
        print(org)
        results[org] = []
        if status in status_requirements:
            sources = get_sources_for_org(org)
            for source in sources:
                if source["__typename"] == typename:
                    results[org].append(source)
    return results


def get_project_ids_for_source(source_id, org):
    project_ids = []
    if org == "teleport":
        return "clj8nlwye0004s60185sa6hj8"
    project_map = get_project_source_map_for_org(org)
    for project_id, sources in project_map.items():
        for source in sources:
            if source["id"] == source_id:
                project_ids.append(project_id)

    return project_ids


def get_sources_with_project_ids(org_alias):
    project_source_map = get_project_source_map_for_org(org_alias)
    sources_with_projects = {}

    for project_id, sources in project_source_map.items():
        for source in sources:
            source_id = source["id"]
            if source_id not in sources_with_projects:
                sources_with_projects[source_id] = {
                    "source": source,
                    "project_ids": set(),
                }
            sources_with_projects[source_id]["project_ids"].add(project_id)

    return [
        {**data["source"], "project_ids": list(data["project_ids"])}
        for data in sources_with_projects.values()
    ]
