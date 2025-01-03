from urllib.parse import urljoin
from typing import Any, Dict, Iterable, Optional
from dlt.sources.helpers import requests
from dlt.common.typing import TDataItems
from .exceptions import EisaApiError


class EisaApi:
    """
    An API client for the EISA platform that can fetch paginated data.
    """

    def __init__(self, base_url: str, username: str, password: str, access_token: Optional[str] = None) -> None:
        self.base_url = base_url.rstrip("/")  # Ensure no trailing slash
        self.username = username
        self.password = password
        self.access_token = access_token

    def get_resources(self) -> Dict[str, str]:
        """Fetch available resources from the EISA API."""
        url = f"{self.base_url}/odata/v1/"
        response = requests.get(url, auth=(self.username, self.password))
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise EisaApiError(
                f"Failed to fetch resources from {url}: {e}",
                status_code=response.status_code,
                response=response.text,
            )

        data = response.json()
        return {item["name"]: item["url"] for item in data.get("value", [])}

    def get_pages(self, resource: str, params: Optional[Dict[str, Any]] = None) -> Iterable[TDataItems]:
        """Fetch paginated data from the EISA API and flatten nested structures."""

        def flatten_and_process_data(data):
            """
            Flattens nested data and ensures follow-up responses are integrated into a single column.

            Args:
                data (dict): A dictionary representing a single row of data.

            Returns:
                dict: A flattened and processed dictionary.
            """
            def flatten_json(data, parent_key='', sep='__'):
                """
                Flattens a nested dictionary into a single level.

                :param data: The dictionary to flatten
                :param parent_key: The base key to append to
                :param sep: Separator to use between keys
                :return: A flattened dictionary
                """
                items = []
                for key, value in data.items():
                    new_key = f"{parent_key}{sep}{key}" if parent_key else key
                    if isinstance(value, dict):
                        items.extend(flatten_json(value, new_key, sep=sep).items())
                    else:
                        items.append((new_key, value))
                return dict(items)

            # Flatten the row (if nested)
            flat_row = flatten_json(data)

            # Handle conditional logic
            primary_question = "environment_peaceful"
            follow_up_question = "response_if_no"

            if flat_row.get(primary_question) == "No":
                follow_up_response = flat_row.pop(follow_up_question, {})
                if isinstance(follow_up_response, dict):
                    concatenated_response = "\\".join(
                        [f"{k}: {v}" for k, v in follow_up_response.items() if v]
                    )
                    flat_row[follow_up_question] = concatenated_response
                else:
                    flat_row[follow_up_question] = follow_up_response

            return flat_row

        url = f"{self.base_url}/odata/v1/{resource}"
        while url:
            try:
                response = requests.get(url, params=params, auth=(self.username, self.password))
                response.raise_for_status()
                json_data = response.json()
                for item in json_data.get("value", []):
                    yield flatten_and_process_data(item)  # Flatten and process data before yielding
                url = json_data.get("@odata.nextLink")
            except Exception as e:
                raise EisaApiError(f"Error fetching data from {url}: {e}")
