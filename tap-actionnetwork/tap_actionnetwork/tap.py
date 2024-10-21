"""TapActionNetwork tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_actionnetwork import streams


class TapTapActionNetwork(Tap):
    """TapActionNetwork tap class."""

    name = "tap-actionnetwork"
    replication_key = "modified_date"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TapActionNetworkStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ContactsStream(self),
        ]


if __name__ == "__main__":
    TapTapActionNetwork.cli()
