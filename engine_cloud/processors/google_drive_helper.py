from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
import os
import tempfile

from engine_cloud.config import settings
from engine_cloud.processors.nango_helper import (
    get_nango_access_refresh_tokens,
    get_nango_connection_metadata,
)
from engine_cloud.processors.reducto_helper import (
    upload_folder,
    convert_and_upload_reducto_results,
)

MIME_TYPE_EXTENSION_MAP = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "text/plain": ".txt",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "application/zip": ".zip",
    "text/csv": ".csv",
    "application/vnd.google-apps.document": ".doc",  # Google Docs
    "application/vnd.google-apps.spreadsheet": ".sheet",  # Google Sheets
    "application/vnd.google-apps.presentation": ".slides",  # Google Slides
    "application/vnd.google-apps.folder": "",  # Folders have no extension
    # Add more MIME types as needed
}

MIME_TYPE_EXPORT_MAP = {
    "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}


def get_google_drive_service(connection_id):
    # Initialize Credentials with the provided tokens and client information
    access_token, refresh_token = get_nango_access_refresh_tokens(
        "google-drive", connection_id
    )
    creds = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=settings.GOOGLE_DRIVE_CLIENT_ID,
        client_secret=settings.GOOGLE_DRIVE_CLIENT_SECRET,
    )

    # Build the Drive API service
    service = build("drive", "v3", credentials=creds)
    return service


def get_file_ids_from_nango(connection_id):
    metadata = get_nango_connection_metadata("google-drive", connection_id)
    return [file["id"] for file in metadata["files"]]


def list_all_files(service, page_size=100):
    """
    List all files the user has access to, handling pagination.
    Args:
        service: Authenticated Google Drive API service instance.
    Returns:
        A list of all files.
    """
    files = []
    page_token = None
    while True:
        response = (
            service.files()
            .list(
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType, parents)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get(
            "nextPageToken", None
        )  # Get the token for the next page
        if not page_token:  # No more pages
            break
    return files


def get_file_metadata(service, file_id):
    file_metadata = service.files().get(fileId=file_id).execute()
    return file_metadata


def download_file(service, file_id, mime_type, destination_path):
    """
    Download a file from Google Drive.
    For Google Docs/Sheets/Slides, export to a specific format.

    Args:
        service: Authenticated Drive API service instance.
        file_id: ID of the file to download.
        mime_type: MIME type to export Docs, Sheets, or Slides.
        destination_path: Local path to save the file.

    Returns:
        None
    """
    try:
        # For binary files (e.g., PDFs, images)
        request = service.files().get_media(fileId=file_id)
        file = BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}.")
        file.seek(0)
        with open(destination_path, "wb") as f:
            f.write(file.getbuffer())
    except Exception as e:
        # Handle Docs/Sheets/Slides export
        if "fileNotDownloadable" in str(e):
            print("File is not directly downloadable. Exporting instead.")
            if mime_type in MIME_TYPE_EXPORT_MAP:
                mime_type = MIME_TYPE_EXPORT_MAP[mime_type]
            request = service.files().export_media(fileId=file_id, mimeType=mime_type)
            file = BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}.")
            file.seek(0)
            with open(destination_path, "wb") as f:
                f.write(file.getbuffer())
        else:
            raise


def download_all_files(connection_id, file_ids):
    service = get_google_drive_service(connection_id)
    temp_dir = tempfile.mkdtemp()
    for file_id in file_ids:
        metadata = get_file_metadata(service, file_id)
        if metadata["mimeType"] in MIME_TYPE_EXPORT_MAP:
            mime_type = MIME_TYPE_EXPORT_MAP[metadata["mimeType"]]
        else:
            mime_type = metadata["mimeType"]
        download_file(
            service,
            file_id,
            mime_type,
            os.path.join(
                temp_dir, f"{metadata['name']}.{MIME_TYPE_EXTENSION_MAP[mime_type]}"
            ),
        )
    return temp_dir


def upload_google_drive_records(
    connection_id, index_name, source_id, org_alias, project_id
):
    file_dir = download_all_files(connection_id)
    results, failed = upload_folder(file_dir)
    convert_and_upload_reducto_results(
        index_name, source_id, org_alias, project_id, results, failed
    )
