import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.acled.acled import Acled


class TestAcled:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    f"dataset-{dataset_name}.json",
                )
            )

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "acled", "config")

    def test_acled(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "TestAcled",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                acled = Acled(configuration, retriever, tempdir)
                acled.download_data(2025)
                assert len(acled.dates) == 6
                assert len(acled.data) == 2
                assert len(acled.data["2025-2029"]) == 14373

                dataset = acled.generate_dataset()
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))
                assert dataset == {
                    "name": "global-acled",
                    "title": "Global ACLED data",
                    "tags": [
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "groups": [{"name": "world"}],
                    "dataset_date": "[1997-01-01T00:00:00 TO 2025-01-10T23:59:59]",
                    "license_id": "hdx-other",
                    "license_other": "By using ACLED data you agree to abide by the "
                    "[Terms of Use and Attribution Policy](https://acleddata.com/terms-of-use/).",
                    "methodology": "Other",
                    "methodology_other": "Please review the [ACLED Codebook]"
                    "(https://acleddata.com/download/2827) for more information about "
                    "event definitions, sub-event definitions, and coding methodology. "
                    "For more information about our fatality and sourcing methodology, "
                    "please review our [Fatality FAQs](https://acleddata.com/acleddatanew/wp-"
                    "content/uploads/dlm_uploads/2020/02/FAQs_-ACLED-Fatality-Methodology_"
                    "2020.pdf) and [Sourcing FAQs](https://acleddata.com/acleddatanew/wp-"
                    "content/uploads/dlm_uploads/2020/02/FAQs_ACLED-Sourcing-Methodology."
                    "pdf). Time period coverage varies by country and region, so please "
                    "consult the ACLED [Coverage List](https://acleddata.com/download/"
                    "4404/). For more methodology resources, click [here]"
                    "(https://acleddata.com/resources/general-guides/).",
                    "caveats": "None",
                    "dataset_source": "ACLED",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "hdx",
                    "data_update_frequency": 7,
                    "notes": "A weekly dataset providing the total number of reported "
                    "civilian targeting events and fatalities, demonstration events, and "
                    "political violence events and fatalities broken down by country.  "
                    "\n\nThese are pulled from three global ACLED datasets: [Civilian "
                    "Targeting Events and Fatalities](https://data.humdata.org/dataset/ci"
                    "vilian-targeting-events-and-fatalities), [Demonstration Events]"
                    "(https://data.humdata.org/dataset/demonstration-events), and "
                    "[Political Violence Events and Fatalities](https://data.humdata.org"
                    "/dataset/political-violence-events-and-fatalities).",
                    "subnational": "1",
                    "dataset_preview": "no_preview",
                }

                resources = dataset.get_resources()
                assert len(resources) == 2
                assert resources[0] == {
                    "name": "conflict_events_and_fatalities for 2025-2029",
                    "description": "A weekly dataset providing the total number of "
                    "reported conflict events and fatalities broken down by country "
                    "and month for 2025-2029.",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                assert filecmp.cmp(
                    join(tempdir, "conflict_events_and_fatalities_2025-2029.csv"),
                    join(fixtures_dir, "conflict_events_and_fatalities_2025-2029.csv"),
                )
