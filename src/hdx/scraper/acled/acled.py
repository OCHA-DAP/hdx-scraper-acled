#!/usr/bin/python
"""acled scraper"""

import logging
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.retriever import Retrieve
from pandas import DataFrame, concat, read_excel

logger = logging.getLogger(__name__)


class Acled:
    def __init__(self, configuration: Configuration, retriever: Retrieve, temp_dir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.dates = []
        self.data = DataFrame()

    def download_data(self):
        for dataset_name in self._configuration["datasets"]:
            event_type = dataset_name[: dataset_name.index("event") - 1]
            event_type = event_type.replace("-", "_")
            dataset = Dataset.read_from_hdx(dataset_name)
            resource = dataset.get_resource(0)
            self.dates.append(dataset.get_time_period()["startdate"])
            self.dates.append(dataset.get_time_period()["enddate"])
            file_path = self._retriever.download_file(resource["url"])
            for sheet_name in self._configuration["sheets"]:
                contents = read_excel(file_path, sheet_name=sheet_name)
                headers = contents.columns
                if "ISO3" not in headers:
                    country_names = contents["Country"]
                    country_isos = [
                        Country.get_iso3_country_code(country)
                        for country in country_names
                    ]
                    contents["ISO3"] = country_isos
                if "Admin1" not in headers:
                    contents["Admin1"] = None
                    contents["Admin2"] = None
                    contents["Admin1 Pcode"] = None
                    contents["Admin2 Pcode"] = None
                if "Fatalities" not in headers:
                    contents["Fatalities"] = None
                contents["Event Type"] = event_type
                months = contents["Month"]
                years = contents["Year"]
                dates = [parse_date_range(f"{m} {y}") for m, y in zip(months, years)]
                contents["Start Date"] = [d[0] for d in dates]
                contents["End Date"] = [d[1] for d in dates]
                contents["Dataset Id"] = dataset["id"]
                contents["Resource Id"] = resource["id"]

                contents.rename(
                    columns={
                        "ISO3": "Country ISO3",
                        "Admin1": "Admin 1 Name",
                        "Admin2": "Admin 2 Name",
                        "Admin1 Pcode": "Admin 1 PCode",
                        "Admin2 Pcode": "Admin 2 PCode",
                    },
                    inplace=True,
                )
                contents = contents[self._configuration["hxl_tags"].keys()]

                if len(self.data) == 0:
                    self.data = contents
                else:
                    self.data = concat([self.data, contents])

    def generate_dataset(self) -> Optional[Dataset]:
        dataset = Dataset(
            {
                "name": "global-acled",
                "title": "Global ACLED data",
            }
        )
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")
        start_date = min(self.dates)
        end_date = max(self.dates)
        dataset.set_time_period(start_date, end_date)

        self.data = self.data.to_dict(orient="records")
        resourcedata = {
            "name": "conflict_events_and_fatalities",
            "description": "A weekly dataset providing the total number of reported "
            "events and fatalities broken down by country and month.",
        }
        hxl_tags = self._configuration["hxl_tags"]
        headers = list(hxl_tags.keys())
        dataset.generate_resource_from_iterable(
            headers,
            self.data,
            hxl_tags,
            self._temp_dir,
            "conflict_events_and_fatalities.csv",
            resourcedata,
            encoding="utf-8-sig",
        )

        return dataset
