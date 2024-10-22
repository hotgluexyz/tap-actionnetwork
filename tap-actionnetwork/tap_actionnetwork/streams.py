"""Stream type classes for tap-actionnetwork."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_actionnetwork.client import TapActionNetworkStream

class ContactsStream(TapActionNetworkStream):
    """Define contacts stream."""

    name = "contacts"
    path = "/people"
    replication_key = "modified_date"
    schema = th.PropertiesList(
        th.Property("given_name", th.StringType),
        th.Property("family_name", th.StringType),
        th.Property("identifiers", th.ArrayType(th.StringType)),
        th.Property("email_addresses", th.ArrayType(th.ObjectType(
                th.Property("address", th.StringType),
                th.Property("primary", th.BooleanType),
                th.Property("status", th.StringType),
            ),
        )),
        th.Property(
            "phone_numbers",
            th.ArrayType(
                th.ObjectType(
                    th.Property("number", th.StringType),
                    th.Property("primary", th.BooleanType),
                    th.Property("status", th.StringType),
                    th.Property("number_type", th.StringType),
                ),
            ),
        ),
        th.Property(
            "postal_addresses",
            th.ArrayType(
                th.ObjectType(
                    th.Property("primary", th.BooleanType),
                    th.Property("locality", th.StringType),
                    th.Property("region", th.StringType),
                    th.Property("postal_code", th.StringType),
                    th.Property("country", th.StringType),
                    th.Property("location", th.ObjectType(
                        th.Property("latitude", th.NumberType),
                        th.Property("longitude", th.NumberType),
                        th.Property("accuracy", th.StringType),
                    )),
                    th.Property("address_lines", th.ArrayType(th.StringType)),
                ),
            ),
        ),
        th.Property("_links", th.StringType),  # Treat as JSON string
        th.Property("custom_fields", th.StringType),  # Treat as JSON string
        th.Property("created_date", th.StringType),
        th.Property("modified_date", th.StringType),
        th.Property("languages_spoken", th.ArrayType(th.StringType)),
    ).to_dict()
