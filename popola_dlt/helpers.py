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
        """Fetch paginated data from the EISA API."""
        url = f"{self.base_url}/odata/v1/{resource}"
        while url:
            try:
                response = requests.get(url, params=params, auth=(self.username, self.password))
                response.raise_for_status()
                json_data = response.json()
                yield json_data.get("value", [])
                url = json_data.get("@odata.nextLink")
            except Exception as e:
                break
