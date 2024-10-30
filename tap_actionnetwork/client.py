"""REST client handling, including TapActionNetworkStream base class."""

from __future__ import annotations

from typing import Any, Iterable
from urllib.parse import urlparse, parse_qs

from requests import Response
import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream


class ActionNetworkPaginator:
    def get_next(self, response):
        next_link = response.json().get("_links", {}).get("next", False)
        if self.has_more(response):
            query_params = parse_qs(urlparse(next_link).query)
            return int(query_params.get("page", [1])[0]) + 1
        return None
    
    def has_more(self, response: Response) -> bool:
        """Return True if there are more pages available."""
        return response.json().get("_links", {}).get("next", False)


class TapActionNetworkStream(RESTStream):
    """TapActionNetwork stream class."""

    records_jsonpath = "$._embedded[*]"
    next_page_token_jsonpath = "$._links.next.href"  # noqa: S105
    pages_to_consider = 10

    @property
    def schema(self) -> str:
        return super().schema

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://actionnetwork.org/api/v2/"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="OSDI-API-Token",
            value=self.config.get("token", ""),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers
    
    def get_new_paginator(self):
        return ActionNetworkPaginator(start_value=1)

    def get_url_params(
        self,
        context: Any | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["filter"] = f"{self.replication_key} gt {date}"
        return params

    def prepare_request_payload(
        self,
        context: Any | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: Any | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
