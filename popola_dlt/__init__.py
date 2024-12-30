import dlt
from typing import Iterable, Optional
from dlt.sources import DltResource
from .helpers import EisaApi


@dlt.source(name="eisa")
def eisa_source(
    access_token: Optional[str] = dlt.secrets.value,
    username: Optional[str] = dlt.secrets.value,
    password: Optional[str] = dlt.secrets.value,
    base_url: str = dlt.config.value,
    start_date: str = "2000-01-01",
) -> Iterable[DltResource]:
    client = EisaApi(
        base_url=base_url,
        username=username,
        password=password,
        access_token=access_token,
    )

    resources_info = client.get_resources()

    for resource_name, resource_url in resources_info.items():
        try:
            @dlt.resource(name=resource_name, primary_key="ResponseID", write_disposition="merge")
            def dynamic_resource() -> Iterable:
                params = {"$top": 100}

                # Add ordering if ResponseSubmitDate exists
                if "ResponseSubmitDate" in resource_url:
                    params["$orderby"] = "ResponseSubmitDate asc"

                data = list(client.get_pages(resource_url, params))
                yield from data

            yield dynamic_resource
        except Exception as e:
            pass
