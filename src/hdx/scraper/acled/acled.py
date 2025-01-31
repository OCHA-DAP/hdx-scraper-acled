#!/usr/bin/python
"""acled scraper"""

import logging
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.retriever import Retrieve
from pandas import concat, read_excel

logger = logging.getLogger(__name__)


class Acled:
    def __init__(self, configuration: Configuration, retriever: Retrieve, temp_dir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.dates = []
        self.data = {}

    def download_data(self, year: int) -> None:
        for dataset_name in self._configuration["datasets"]:
            event_type = dataset_name[: dataset_name.index("event") - 1]
            event_type = event_type.replace("-", "_")

            # Download and read data
            dataset = Dataset.read_from_hdx(dataset_name)
            resource = dataset.get_resource(0)
            self.dates.append(dataset.get_time_period()["startdate"])
            self.dates.append(dataset.get_time_period()["enddate"])
            file_path = self._retriever.download_file(resource["url"])
            for sheet_name in self._configuration["sheets"]:
                contents = read_excel(file_path, sheet_name=sheet_name)
                headers = contents.columns

                # Add ISO codes, HRP and GHO status
                country_names = contents["Country"]
                country_isos = []
                hrps = []
                ghos = []
                for i in range(len(country_names)):
                    if "ISO3" in headers:
                        country_iso = contents["ISO3"][i]
                    else:
                        country_iso = Country.get_iso3_country_code_fuzzy(
                            country_names[i]
                        )[0]
                    if country_names[i] == "Kosovo":
                        country_iso = "XKX"
                    hrp = Country.get_hrp_status_from_iso3(country_iso)
                    gho = Country.get_gho_status_from_iso3(country_iso)
                    if hrp is None:
                        hrp = False
                    if gho is None:
                        gho = False
                    country_isos.append(country_iso)
                    hrps.append(hrp)
                    ghos.append(gho)
                contents["ISO3"] = country_isos
                contents["has_hrp"] = hrps
                contents["in_gho"] = ghos

                # Add admin columns
                if "Admin1" not in headers:
                    contents["Admin1"] = None
                    contents["Admin2"] = None
                    contents["Admin1 Pcode"] = None
                    contents["Admin2 Pcode"] = None
                    contents["admin_level"] = 0
                else:
                    contents["admin_level"] = 2

                # Add fatalities column
                if "Fatalities" not in headers:
                    contents["Fatalities"] = None
                contents["event_type"] = event_type

                # Add reference period
                months = contents["Month"]
                years = contents["Year"]
                dates = [parse_date_range(f"{m} {y}") for m, y in zip(months, years)]
                contents["reference_period_start"] = [d[0] for d in dates]
                contents["reference_period_end"] = [d[1] for d in dates]

                # Add original dataset and resource ids
                contents["dataset_id"] = dataset["id"]
                contents["resource_id"] = resource["id"]

                contents.rename(
                    columns={
                        "ISO3": "location_code",
                        "Admin1": "admin1_name",
                        "Admin2": "admin2_name",
                        "Admin1 Pcode": "admin1_code",
                        "Admin2 Pcode": "admin2_code",
                        "Fatalities": "fatalities",
                        "Events": "events",
                    },
                    inplace=True,
                )

                # Split data by years
                for year_start in range(1995, year + 1, 5):
                    year_end = year_start + 4
                    year_range = f"{year_start}-{year_end}"
                    subset = contents.loc[
                        (contents["Year"] >= year_start) & (contents["Year"] <= year_end),
                        self._configuration["hxl_tags"].keys(),
                    ]
                    if len(subset) == 0:
                        continue

                    if year_range in self.data:
                        self.data[year_range] = concat([self.data[year_range], subset])
                    else:
                        self.data[year_range] = subset
        return

    def generate_dataset(self) -> Optional[Dataset]:
        dataset = Dataset(
            {
                "name": "hdx-hapi-conflict-event-test",
                "title": "HDX HAPI - Coordination & Context: Conflict Events",
            }
        )
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")
        start_date = min(self.dates)
        end_date = max(self.dates)
        dataset.set_time_period(start_date, end_date)

        hxl_tags = self._configuration["hxl_tags"]
        headers = list(hxl_tags.keys())
        for date_range in reversed(self.data.keys()):
            data = self.data[date_range].to_dict(orient="records")
            resourcedata = {
                "name": self._configuration["resource_name"].replace(
                    "date_range", date_range
                ),
                "description": self._configuration["resource_description"].replace(
                    "date_range", date_range
                ),
            }
            dataset.generate_resource_from_iterable(
                headers,
                data,
                hxl_tags,
                self._temp_dir,
                f"hdx_hapi_conflict_event_global_{date_range}.csv",
                resourcedata,
                encoding="utf-8-sig",
            )

        return dataset
