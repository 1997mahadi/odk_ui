import dlt
from typing import Iterable, Optional
from dlt.sources import DltResource
from .helpers import EisaApi


@dlt.source(name="eisa", max_table_nesting=0)
def eisa_source(
    access_token: Optional[str] = dlt.secrets.value,
    username: Optional[str] = dlt.secrets.value,
    password: Optional[str] = dlt.secrets.value,
    base_url: str = dlt.config.value,
    start_date: str = "2000-01-01",
) -> Iterable[DltResource]:
    """
    A DLT source to fetch data from the EISA API.
    Iterates over resources and yields DLT resources for loading into a destination.

    :param access_token: Optional API access token
    :param username: Username for API authentication
    :param password: Password for API authentication
    :param base_url: Base URL of the EISA API
    :param start_date: Start date for filtering records
    :return: Iterable of DltResource
    """
    # Initialize API client
    client = EisaApi(
        base_url=base_url,
        username=username,
        password=password,
        access_token=access_token,
    )

    # Fetch all available resources from the API
    resources_info = client.get_resources()

    # Iterate over each resource and create a dynamic DLT resource
    for resource_name, resource_url in resources_info.items():
        try:
            @dlt.resource(name=resource_name, primary_key="ResponseID", write_disposition="merge", max_table_nesting=0)
            def dynamic_resource() -> Iterable:
                """
                Fetch paginated and preprocessed data from the resource and yield as rows.
                """
                # Parameters for fetching data
                params = {}

                # Add ordering if ResponseSubmitDate exists in the resource
                if "ResponseSubmitDate" in resource_url:
                    params["$orderby"] = "ResponseSubmitDate asc"

                # Fetch data using pagination (already flattened)
                for page in client.get_pages(resource_url, params):
                    yield from page

            # Yield the dynamic resource for processing
            yield dynamic_resource
        except Exception as e:
            # Log the error for debugging
            print(f"Error processing resource {resource_name}: {e}")
            continue
