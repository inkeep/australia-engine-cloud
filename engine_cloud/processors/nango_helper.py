import json
import requests
import backoff

from engine_cloud.config import settings


@backoff.on_exception(backoff.expo, Exception, max_time=60, max_tries=5)
def get_nango_key(integration_id, connection_id):
    headers = {"Authorization": f"Bearer {settings.NANGO_API_KEY}"}

    params = {"provider_config_key": integration_id}

    response = requests.get(
        f"https://connect.inkeep.com/connection/{connection_id}",
        headers=headers,
        params=params,
    )

    return response.json()["credentials"]["access_token"]


def get_nango_connection_metadata(integration_id, connection_id):
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
    }
    querystring = {"provider_config_key": integration_id, "refresh_token": "true"}

    response = requests.get(
        f"https://connect.inkeep.com/connection/{connection_id}",
        headers=headers,
        params=querystring,
    )

    data = response.json()["metadata"]
    return data


def get_nango_access_refresh_tokens(integration_id, connection_id):
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
    }
    querystring = {"provider_config_key": integration_id, "refresh_token": "true"}

    response = requests.get(
        f"https://connect.inkeep.com/connection/{connection_id}",
        headers=headers,
        params=querystring,
    )

    data = response.json()
    credentials = data["credentials"]
    refresh_token = (
        credentials["refresh_token"] if "refresh_token" in credentials else None
    )

    return credentials["access_token"], refresh_token


def get_nango_connection_ids_by_type(integration_typename):
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
        "Provider-Config-Key": integration_typename,
    }

    response = requests.get(
        f"https://connect.inkeep.com/connection",
        headers=headers,
    )
    response.raise_for_status()
    return [
        x["connection_id"]
        for x in response.json()["connections"]
        if x["provider_config_key"] == integration_typename
    ]


def get_zendesk_subdomain(connection_id):
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
    }
    querystring = {"provider_config_key": "zendesk"}

    response = requests.get(
        f"https://connect.inkeep.com/connection/{connection_id}",
        headers=headers,
        params=querystring,
    )
    return response.json()["connection_config"]["subdomain"]


def set_nango_metadata():
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
    }
    payload = {
        "connection_id": "test-connection-id",
        "provider_config_key": "sharepoint-online",
        "metadata": {
            "sitesToSync": [
                {
                    "id": "inkeep.sharepoint.com,0b28fc9e-c4bf-4f37-a0ae-7d4bbb072ee7,58d04f61-a3db-400c-99bd-1906d93cb754"
                }
            ]
        },
    }

    response = requests.post(
        "https://api.nango.dev/connection/metadata", json=payload, headers=headers
    )


def start_syncs():
    url = "https://api.nango.dev/sync/start"

    payload = {
        "provider_config_key": "sharepoint-online",
        "connection_id": "test-connection-id",
        "syncs": ["file-sync"],
    }
    headers = {
        "Authorization": f"Bearer {settings.NANGO_API_KEY}",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    print(response.text)
