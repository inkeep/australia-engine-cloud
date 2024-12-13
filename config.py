import typing as T

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    INKEEP_MANAGEMENT_GRAPHQL_API_URL: str
    INKEEP_MANAGEMENT_GRAPHQL_API_KEY: str

    MELBOURNE_API_URL: str
    MELBOURNE_API_KEY: str

    MONGODB_URL: str

    GITHUB_TOKEN: str
    GITHUB_TOKEN_ALTERNATE: str
    GITHUB_PAT: str

    BASETEN_NOTION_TOKEN: str
    BASETEN_PUBLIC_NOTION_TOKEN: str
    XANO_NOTION_TOKEN: str
    CLERK_NOTION_TOKEN: str
    BANGERT_NOTION_TOKEN: str

    INTELYCARE_CONFLUENCE_TOKEN: str

    NANGO_API_KEY: str

    DISCORD_BOT_TOKEN: str

    SLACK_SKYFLOW_KEY: str
    SLACK_DEVMARKETING_KEY: str
    SLACK_INKEEP_KEY: str
    SLACK_TOSS_INTERNAL_KEY: str
    SLACK_DELIVERLOGIC_KEY: str
    SLACK_DEEPHAVEN_KEY: str
    SLACK_DEEPHAVEN_INTERNAL_KEY: str
    ANTHROPIC_API_KEY: str

    INDEX_AUTO_SOURCE_SYNC_URL: str

    class Config:
        env_file = ".env"


settings = Settings()  # type: ignore
