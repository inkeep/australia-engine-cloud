import time
import typing as T
from enum import Enum

from pydantic import BaseModel

import toolz
from fastapi.encoders import jsonable_encoder
from pymongo import DeleteOne, InsertOne, MongoClient, UpdateOne

from engine_cloud.config import settings


class RecordFormat(Enum):
    MD = "md"
    MDX = "mdx"
    HTML = "html"
    PDF = "pdf"
    YML = "yml"
    TXT = "txt"
    EVOLVABLE_ENUM_VALUE = "evolvable_enum_value"


class SourceTypes(Enum):
    SITE = "site"
    DOCS = "docs"
    SUPPORT = "support"
    GITHUB = "github"


class MongoRecordMetadata(BaseModel):
    organization: str
    source_type: str
    source_id: str
    project_id: str
    index_name: str
    pinecone_index_name: T.Optional[str] = None
    indexing_engine: T.Optional[str] = None
    demo_token: str = ""


class MongoRecordMetadataQueryInput(BaseModel):
    organization: T.Optional[str] = None
    source_type: T.Optional[str] = None
    source_id: T.Optional[str] = None
    project_id: T.Optional[str] = None
    index_name: T.Optional[str] = None
    pinecone_index_name: T.Optional[str] = None
    indexing_engine: T.Optional[str] = None


class MongoRecord(BaseModel):
    record_id: str
    format: RecordFormat = "md"
    content: str
    url: T.Optional[str] = None
    title: T.Optional[str] = None
    attributes: T.Optional[dict] = None
    extra_attributes: T.Optional[dict] = None
    metadata: T.Optional[MongoRecordMetadata] = None


def check_json_structure(obj):
    if not isinstance(obj, dict):
        raise ValueError("JSON has to be a dictionary")

    for key in obj.keys():
        if not isinstance(key, str):
            raise ValueError("JSON keys have to be strings")

    for value in obj.values():
        if not isinstance(value, (str, int, float, bool, list)):
            raise ValueError(
                "JSON values have to be strings, numbers, booleans, or lists"
            )
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, (str, int, float, bool)):
                    raise ValueError(
                        "JSON list values have to be strings, numbers, or booleans"
                    )


class IndexingEngine(Enum):
    GENERIC_DOCS = "generic_docs"
    GENERIC_PDFS = "generic_pdfs"
    MINTLIFY_DOCS = "mintlify_docs"
    GITHUB_PULL_REQUESTS = "github_pull_requests"
    GITHUB_ISSUES = "github_issues"
    GITHUB_DISCUSSIONS = "github_discussions"
    GITHUB_READMES = "github_readmes"
    GITHUB_RELEASES = "github_releases"
    STACK_OVERFLOW_QUESTIONS = "stack_overflow_questions"
    DISCOURSE_POSTS = "discourse_posts"
    DISCORD_MESSAGES = "discord_messages"
    DISCORD_FORUM_POSTS = "discord_forum_posts"
    SLACK_MESSAGES = "slack_messages"
    OPENAPI = "openapi"
    INKEEP_CUSTOM_QUESTION_ANSWERS = "inkeep_custom_question_answers"
    EVOLVABLE_ENUM_VALUE = "evolvable_enum_value"


def get_mongo_client():
    return MongoClient(settings.MONGODB_URL)


def get_metadata_parameters(metadata: MongoRecordMetadataQueryInput):
    metadata = jsonable_encoder(metadata)
    delete_fields = []
    if metadata["organization"]:
        delete_fields.append({"metadata.organization": metadata["organization"]})
    if metadata["source_type"]:
        delete_fields.append({"metadata.source_type": metadata["source_type"]})
    if metadata["source_id"]:
        delete_fields.append({"metadata.source_id": metadata["source_id"]})
    if metadata["project_id"]:
        delete_fields.append({"metadata.project_id": metadata["project_id"]})
    if metadata["index_name"]:
        delete_fields.append({"metadata.index_name": metadata["index_name"]})
    if metadata["pinecone_index_name"]:
        delete_fields.append(
            {"metadata.pinecone_index_name": metadata["pinecone_index_name"]}
        )
    if metadata["indexing_engine"]:
        delete_fields.append({"metadata.indexing_engine": metadata["indexing_engine"]})

    return delete_fields


def preprocess_dict_records(records):
    records = [MongoRecord(**record) for record in records]
    return records


def get_mongo_records_by_metadata(
    input: MongoRecordMetadataQueryInput,
) -> T.List[MongoRecord]:
    client = get_mongo_client()
    db = client.newcastle
    metadata_input = get_metadata_parameters(input)
    if metadata_input:
        records = db["records"].find({"$and": metadata_input})
        records = [record for record in records]
    else:
        records = []

    print(f"found {len(records)} records")
    return records


def get_mongo_records_by_index_name(index_name: str) -> T.List[MongoRecord]:
    client = get_mongo_client()
    db = client.newcastle
    return get_mongo_records_by_metadata(
        MongoRecordMetadataQueryInput(index_name=index_name)
    )


def update_mongodb_records(records: T.List[MongoRecord]) -> int:
    client = get_mongo_client()
    db = client.newcastle
    operations = []
    for record in records:
        metadata = jsonable_encoder(record["metadata"])
        record = jsonable_encoder(record)
        if record["attributes"]:
            check_json_structure(record["attributes"])
        if record["extra_attributes"]:
            check_json_structure(record["extra_attributes"])
        record["metadata"] = metadata
        found = db["records"].find_one(
            {
                "record_id": record["record_id"],
                "metadata.index_name": record["metadata"]["index_name"],
            }
        )
        if found:
            print("adding update")
            operations.append(
                UpdateOne(
                    {
                        "record_id": record["record_id"],
                        "metadata.index_name": record["metadata"]["index_name"],
                    },
                    {"$set": record},
                )
            )
        else:
            print("adding insert")
            operations.append(InsertOne(record))
    try:
        result = db["records"].bulk_write(operations)
    except:
        print(record["url"])
        return None
    return (result.modified_count + result.inserted_count,)


def delete_mongodb_records_by_metadata(input: MongoRecordMetadataQueryInput) -> int:
    client = get_mongo_client()
    db = client.newcastle
    delete_fields = get_metadata_parameters(input)
    if delete_fields:
        result = db["records"].delete_many({"$and": delete_fields})
        count = result.deleted_count
    else:
        count = 0

    return count


def delete_mongodb_records_by_id(record_ids: T.List[str]) -> int:
    client = get_mongo_client()
    db = client.newcastle
    result = db["records"].delete_many({"record_id": {"$in": record_ids}})
    if result.deleted_count:
        print(f"deleted {result.deleted_count} records")
    else:
        print("no records deleted")
    return result.deleted_count


def delete_mongodb_records_by_index_name(index_name: str) -> int:
    metadata = MongoRecordMetadataQueryInput(index_name=index_name)
    client = get_mongo_client()
    db = client.newcastle
    delete_fields = get_metadata_parameters(metadata)
    if delete_fields:
        result = db["records"].delete_many({"$and": delete_fields})
        count = result.deleted_count
    else:
        count = 0

    return count


def upload_batches(
    records,
    write_index_id,
    source_id,
    org_alias,
    project_id,
    piencone_index_name="",
    indexing_engine="",
    source_type="",
    batch_size=10,
):
    print(len(records))
    batches = list(toolz.itertoolz.partition_all(batch_size, records))
    print(len(batches))
    counter = 0
    metadata = MongoRecordMetadata(
        source_id=source_id,
        organization=org_alias,
        index_name=write_index_id,
        pinecone_index_name=piencone_index_name,
        indexing_engine=indexing_engine,
        source_type=source_type,
        project_id=project_id,
    )
    mongo_records = []
    for record in records:
        record["metadata"] = metadata
        mongo_records.append(MongoRecord(**record))
    for batch in batches:
        start = time.time()
        update_mongodb_records(list(batch))
        end = time.time()
        print(f"Batch {counter} took {end-start} seconds")
        counter += 1
    print("sucessfully populated index")


def delete_mongodb_records_by_id(record_ids) -> int:
    client = get_mongo_client()
    db = client.newcastle
    operations = []
    for record_id in record_ids:
        operations.append(DeleteOne({"record_id": record_id}))

    result = db["records"].bulk_write(operations)
    return result.deleted_count
