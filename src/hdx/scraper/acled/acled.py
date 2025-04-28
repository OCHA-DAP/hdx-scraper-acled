#!/usr/bin/python
"""acled scraper"""

import logging
from typing import Optional

import numpy as np
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date_range
from hdx.utilities.retriever import Retrieve
from pandas import concat, read_excel

logger = logging.getLogger(__name__)


class Acled:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        error_handler: HDXErrorHandler,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._admins = []
        self._error_handler = error_handler
        self.data = {}
        self.dates = []

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            dataset = admin.get_libhxl_dataset(retriever=self._retriever)
            admin.setup_from_libhxl_dataset(dataset)
            admin.load_pcode_formats()
            self._admins.append(admin)

    def download_data(self, current_year: int) -> None:
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
                contents["event_type"] = event_type
                contents.replace(np.nan, None, inplace=True)

                # Add admin columns
                if "Admin1" not in headers:
                    contents["Admin1"] = None
                    contents["Admin2"] = None
                    contents["Admin1 Pcode"] = None
                    contents["Admin2 Pcode"] = None
                    admin_level = 0
                else:
                    admin_level = 2
                contents["admin_level"] = admin_level

                # Check for duplicates and missing p-codes
                subset = contents[
                    ["Admin2 Pcode", "Admin1", "Admin2", "event_type", "Month", "Year"]
                ]
                subset.loc[contents["Admin2 Pcode"].isna(), "Admin2 Pcode"] = (
                    contents.loc[contents["Admin2 Pcode"].isna(), "Country"]
                )
                duplicates = subset.duplicated(keep=False)
                contents["warning"] = None
                contents["error"] = None
                contents.loc[duplicates, "error"] = "Duplicate row"
                if admin_level == 0:
                    contents.loc[contents["Country"] == "Kosovo", "error"] = (
                        "Non matching p-code"
                    )
                if sum(duplicates) > 0:
                    self._error_handler.add_message(
                        "ACLED",
                        dataset_name,
                        f"{sum(duplicates)} duplicates found in {sheet_name} sheet",
                        resource["name"],
                        err_to_hdx=True,
                    )

                # Loop through rows to check pcodes, get ISO, HRP/GHO status, dates
                country_isos = []
                hrps = []
                ghos = []
                start_dates = []
                end_dates = []
                adm1_pcodes = []
                adm2_pcodes = []
                adm1_names = []
                adm2_names = []
                warnings = []

                for i in range(len(contents)):
                    row = contents.iloc[i]
                    # Get ISO code, HRP and GHO status
                    if "ISO3" in headers:
                        country_iso = row["ISO3"]
                    else:
                        country_iso = Country.get_iso3_country_code_fuzzy(
                            row["Country"]
                        )[0]
                    if row["Country"] == "Kosovo":
                        country_iso = "XKX"
                    hrp = Country.get_hrp_status_from_iso3(country_iso)
                    gho = Country.get_gho_status_from_iso3(country_iso)
                    hrp = "Y" if hrp else "N"
                    gho = "Y" if gho else "N"
                    country_isos.append(country_iso)
                    hrps.append(hrp)
                    ghos.append(gho)

                    month = row["Month"]
                    year = row["Year"]
                    start_date, end_date = parse_date_range(f"{month} {year}")
                    start_dates.append(iso_string_from_datetime(start_date))
                    end_dates.append(iso_string_from_datetime(end_date))

                    # Fill in pcodes and names
                    adm_codes = [None, None]
                    adm_names = [None, None]
                    warning = None

                    if admin_level == 2:
                        provider_adm_names = [row["Admin1"], row["Admin2"]]
                        adm_codes = [row["Admin1 Pcode"], row["Admin2 Pcode"]]
                        adm_names = ["", ""]
                        adm_level, warning = complete_admins(
                            self._admins,
                            country_iso,
                            provider_adm_names,
                            adm_codes,
                            adm_names,
                            fuzzy_match=False,
                        )
                        for w in warning:
                            self._error_handler.add_message(
                                "ACLED",
                                dataset_name,
                                w,
                                message_type="warning",
                            )
                        warning = "|".join(warning)
                    adm1_pcodes.append(adm_codes[0])
                    adm2_pcodes.append(adm_codes[1])
                    adm1_names.append(adm_names[0])
                    adm2_names.append(adm_names[1])
                    warnings.append(warning)

                contents["ISO3"] = country_isos
                contents["has_hrp"] = hrps
                contents["in_gho"] = ghos
                contents["reference_period_start"] = start_dates
                contents["reference_period_end"] = end_dates
                contents["admin1_code"] = adm1_pcodes
                contents["admin2_code"] = adm2_pcodes
                contents["admin1_name"] = adm1_names
                contents["admin2_name"] = adm2_names
                contents["warning"] = warnings

                # Add fatalities column
                if "Fatalities" not in headers:
                    contents["Fatalities"] = None

                # Add original dataset and resource ids
                contents["dataset_hdx_id"] = dataset["id"]
                contents["resource_hdx_id"] = resource["id"]

                contents.rename(
                    columns={
                        "ISO3": "location_code",
                        "Admin1": "provider_admin1_name",
                        "Admin2": "provider_admin2_name",
                        "Fatalities": "fatalities",
                        "Events": "events",
                    },
                    inplace=True,
                )

                # Split data by years
                for year in range(min(contents["Year"]), current_year + 1):
                    subset = contents.loc[
                        contents["Year"] == year,
                        self._configuration["hxl_tags"].keys(),
                    ]
                    if len(subset) == 0:
                        continue

                    if year in self.data:
                        self.data[year] = concat([self.data[year], subset])
                    else:
                        self.data[year] = subset
        return

    def generate_dataset(self) -> Optional[Dataset]:
        dataset = Dataset(
            {
                "name": "hdx-hapi-conflict-event",
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
        for year in reversed(self.data.keys()):
            data = self.data[year].to_dict(orient="records")
            resourcedata = {
                "name": self._configuration["resource_name"].replace("year", str(year)),
                "description": self._configuration["resource_description"].replace(
                    "year", str(year)
                ),
            }
            dataset.generate_resource_from_iterable(
                headers,
                data,
                hxl_tags,
                self._temp_dir,
                f"hdx_hapi_conflict_event_global_{year}.csv",
                resourcedata,
                encoding="utf-8-sig",
            )

        return dataset
