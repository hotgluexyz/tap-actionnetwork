"""TapActionNetwork tap class."""

from __future__ import annotations
import json
import os

import requests
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_actionnetwork import streams

from typing import List
from concurrent.futures import ThreadPoolExecutor
from collections.abc import MutableMapping

class TapTapActionNetwork(Tap):
    """TapActionNetwork tap class."""

    name = "tap-actionnetwork"
    replication_key = "modified_date"
    resources = "https://actionnetwork.org/api/v2/"
    records_jsonpath = "$._embedded[*]"
    next_page_token_jsonpath = "$._links.next.href"  # noqa: S105
    pages_to_consider = 10

    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
    ).to_dict()

    def flatten(self, d: MutableMapping, parent_key: str = '', sep: str = '.') -> dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                for item in v:
                    items.extend(self.flatten(item, new_key + f"{sep}list_item", sep=sep).items())
            elif isinstance(v, list) and v:
                items.append((new_key + f"{sep}list", v))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def get_schema_from_data_sample(self, data_sample: list[dict]) -> dict:
        if data_sample:
            planified_dict = self.planify_list_of_dicts(data_sample)
            return th.PropertiesList(
                *[th.Property(key, th.StringType) for key in planified_dict.keys()]
            ).to_dict()
        return th.PropertiesList().to_dict()

    def planify_list_of_dicts(self, data_sample: list[dict]) -> dict:
        planified_dict = {}
        for item in data_sample:
            planified_dict.update(self.flatten(item))
        return planified_dict
    
    def discover_streams(self) -> List[streams.TapActionNetworkStream]:
        payload = {}
        headers = {
            'Content-Type': 'application/json',
            'OSDI-API-Token': self.config.get("token", "")
        }

        response = requests.request("GET", self.resources, headers=headers, data=payload)
        response.raise_for_status()
        
        data = response.json()
        links = data.get("_links", {})
        valid_prefixes = {curie.get("name") for curie in links.get("curies", [])}
        
        dynamically_discovered_streams = {}
        for possible_stream in links:
            pieces = possible_stream.split(":") if ":" in possible_stream else None
            if pieces and pieces[0] in valid_prefixes:
                url = links[possible_stream].get("href")
                if url:
                    dynamically_discovered_streams[pieces[1]] = {"name": pieces[1], "url": url}
        
        data_samples = {}
        mapped_urls = []
        for stream in dynamically_discovered_streams:
            data_samples[stream] = []
            url = dynamically_discovered_streams[stream].get("url")
            if url in mapped_urls:
                continue
            mapped_urls.append(url)
            while True:
                response = requests.request("GET", url, headers=headers, data=payload)
                response.raise_for_status()
                response_json = response.json()
                self.get_data_samples(data_samples, stream, response_json)
                if "_links" in response_json and "next" in response_json["_links"]:
                    next_link = response_json["_links"].get("next", {}).get("href")
                    if not next_link:
                        break
                    url = next_link
                else:
                    break
            if not data_samples[stream]:
                data_sample = {}
                current_dir = os.path.dirname(__file__)
                sample_dir = os.path.join(current_dir, "samples")
                try:
                    sample_file_path = os.path.join(sample_dir, f"{stream}.json")
                    with open(sample_file_path, "r") as file:
                        data_sample = json.load(file)
                    self.get_data_samples(data_samples, stream, data_sample)
                except FileNotFoundError:
                    self.logger.error(f"Sample file for stream '{stream}' not found.")
                    sub_sample_find = False
                    for filename in os.listdir(sample_dir):
                        if stream in filename and filename.endswith(".json"):
                            try:
                                with open(os.path.join(sample_dir, filename), "r") as file:
                                    data_sample = json.load(file)
                                if data_sample:
                                    sub_sample_find = True
                                    self.get_data_samples(data_samples, stream, data_sample)
                            except json.JSONDecodeError:
                                self.logger.error(f"Error decoding JSON from file '{filename}' for stream '{stream}'.")
                    if not sub_sample_find:
                        self.logger.error(f"No sample data found for stream '{stream}'.")
                except json.JSONDecodeError:
                    self.logger.error(f"Error decoding JSON from sample file for stream '{stream}'.")
                
        return [streams.TapActionNetworkStream(tap=self, name=stream, schema=self.get_schema_from_data_sample(data_samples[stream]), path=dynamically_discovered_streams.get(stream,{}).get("url")) for stream in data_samples]

    def get_data_samples(self, data_samples, stream, response_json):
        if not response_json:
            return
        for record in extract_jsonpath(self.records_jsonpath, input=response_json):
            if isinstance(record, dict) and record:
                for key,element in record.items():
                    if stream not in key:
                        continue
                    data_samples[stream].extend(element)


if __name__ == "__main__":
    TapTapActionNetwork.cli()
