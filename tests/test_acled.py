import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
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

    def test_acled(self, configuration, read_dataset, fixtures_dir, input_dir, config_dir):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestAcled",
                delete_on_success=True,
                delete_on_failure=False,
            ) as temp_folder:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=temp_folder,
                        saved_dir=input_dir,
                        temp_dir=temp_folder,
                        save=False,
                        use_saved=True,
                    )
                    acled = Acled(configuration, retriever, temp_folder, error_handler)
                    acled.get_pcodes()
                    acled.download_data(2025)
                    assert len(acled.dates) == 6
                    assert len(acled.data) == 2
                    assert len(acled.data["2025-2029"]) == 14373

                    dataset = acled.generate_dataset()
                    dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))
                    assert dataset == {
                        "caveats": "",
                        "data_update_frequency": 7,
                        "dataset_date": "[1997-01-01T00:00:00 TO 2025-01-10T23:59:59]",
                        "dataset_preview": "no_preview",
                        "dataset_source": "Armed Conflict Location & Event Data Project "
                        "(ACLED)",
                        "groups": [{"name": "world"}],
                        "license_id": "hdx-other",
                        "license_other": "By using ACLED data you agree to abide by the "
                        "[Terms of Use and Attribution Policy](https://acleddata.com/terms-of-use/).",
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "methodology": "Registry",
                        "name": "hdx-hapi-conflict-event",
                        "notes": "This dataset contains data obtained from the\n"
                        "[HDX Humanitarian API](https://hapi.humdata.org/) (HDX HAPI),\n"
                        "which provides standardized humanitarian indicators designed\n"
                        "for seamless interoperability from multiple sources.\n"
                        "The data facilitates automated workflows and visualizations\n"
                        "to support humanitarian decision making.\n"
                        "For more information, please see the HDX HAPI\n"
                        "[landing page](https://data.humdata.org/hapi)\n"
                        "and\n"
                        "[documentation](https://hdx-hapi.readthedocs.io/en/latest/).\n",
                        "owner_org": "hdx-hapi",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "conflict-violence",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            }
                        ],
                        "title": "HDX HAPI - Coordination & Context: Conflict Events",
                    }

                    resources = dataset.get_resources()
                    assert len(resources) == 2
                    assert resources[0] == {
                        "description": "Conflict Event data from HDX HAPI (2025-2029), "
                        "please see [the documentation](https://hdx-hapi.readthedocs.io/en/"
                        "latest/data_usage_guides/coordination_and_context/#conflict-events) "
                        "for more information",
                        "format": "csv",
                        "name": "Global Coordination & Context: Conflict Events (2025-2029)",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }

                    assert filecmp.cmp(
                        join(temp_folder, "hdx_hapi_conflict_event_global_2025-2029.csv"),
                        join(fixtures_dir, "hdx_hapi_conflict_event_global_2025-2029.csv"),
                    )
