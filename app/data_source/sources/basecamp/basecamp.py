import logging
from datetime import datetime
from typing import Dict, List

import requests
from data_source.api.base_data_source import (
    BaseDataSource,
    BaseDataSourceConfig,
    ConfigField,
    HTMLInputType,
)
from data_source.api.basic_document import BasicDocument, DocumentType
from data_source.api.exception import InvalidDataSourceConfig
from parsers.html import html_to_text
from queues.index_queue import IndexQueue

logger = logging.getLogger(__name__)


class BasecampConfig(BaseDataSourceConfig):
    url: str
    username: str
    password: str


class BasecampClient:
    def __init__(
        self, url: str = None, username: str = None, password: str = None
    ) -> None:
        self.base_url = f"{url}/api/v1"
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers["User-Agent"] = f"GerevAI ({username})"

    def get_projects(self) -> List[Dict]:
        response = self.session.get(f"{self.base_url}/projects.json")
        response.raise_for_status()
        return response.json()

    def get_todos(self, project: Dict) -> List[Dict]:
        url = f"{self.base_url}/projects/{project['id']}/todos.json"

        todos = []
        page = 1

        while True:
            response = self.session.get(url, params={"page": page})
            response.raise_for_status()
            basic_todos = response.json()

            for basic_todo in basic_todos:
                response = self.session.get(basic_todo["url"])
                response.raise_for_status()
                todo = response.json()
                todos.append(todo)

            if not basic_todos:
                break

            page += 1

        return todos


class BasecampDataSource(BaseDataSource):
    @staticmethod
    def get_config_fields() -> List[ConfigField]:
        return [
            ConfigField(label="Basecamp URL", name="url"),
            ConfigField(label="Username", name="username"),
            ConfigField(
                label="Password", name="password", input_type=HTMLInputType.PASSWORD
            ),
        ]

    @staticmethod
    def validate_config(config: Dict) -> None:
        try:
            client = BasecampClient(
                url=config["url"],
                username=config["username"],
                password=config["password"],
            )
            client.get_projects()
        except requests.HTTPError as e:
            raise InvalidDataSourceConfig from e

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = BasecampClient(
            url=self._raw_config["url"],
            username=self._raw_config["username"],
            password=self._raw_config["password"],
        )

    def _feed_new_documents(self) -> None:
        logger.info("Feeding new documents with Basecamp")
        for project in self._client.get_projects():
            self.add_task_to_queue(self._feed_project_issues, project=project)

    def _feed_project_issues(self, project: dict) -> None:
        logging.info(
            f"Getting issues from project {project['name']} ({project['id']})")

        for todo in self._client.get_todos(project):
            children = []

            for comment in todo["comments"]:
                doc = BasicDocument(
                    id=comment["id"],
                    data_source_id=self._data_source_id,
                    type=DocumentType.COMMENT,
                    title=comment["creator"]["name"],
                    content=(
                        html_to_text(comment["content"]
                                     ) if comment["content"] else None
                    ),
                    author=comment["creator"]["name"],
                    author_image_url=comment["creator"]["avatar_url"],
                    location=project["name"],
                    url=f"{todo['app_url']}#comment_{comment['id']}",
                    timestamp=datetime.strptime(
                        comment["updated_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    ),
                )
                children.append(doc)

            doc = BasicDocument(
                id=todo["id"],
                data_source_id=self._data_source_id,
                type=DocumentType.DOCUMENT,
                title=todo["creator"]["name"],
                content=html_to_text(todo["content"]),
                author=todo["creator"]["name"],
                author_image_url=todo["creator"]["avatar_url"],
                location=project["name"],
                url=todo["app_url"],
                timestamp=datetime.strptime(
                    todo["updated_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
                ),
                children=children,
            )
            IndexQueue.get_instance().put_single(doc=doc)
