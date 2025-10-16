from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.acled.pipeline import Pipeline


class TestAcled:
    def test_acled(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
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
                    acled = Pipeline(
                        configuration, retriever, temp_folder, error_handler
                    )
                    acled.get_pcodes()
                    acled.download_data(2025)
                    assert len(acled.dates) == 6
                    assert len(acled.data) == 2
                    assert len(acled.data[2025]) == 14373

                    dataset = acled.generate_dataset()
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
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
                        "[documentation](https://hdx-hapi.readthedocs.io/en/latest/).\n"
                        "\n"
                        "Warnings typically indicate corrections have been made to\n"
                        "the data or show things to look out for. Rows with only warnings\n"
                        "are considered complete, and are made available via the API.\n"
                        "Errors usually mean that the data is incomplete or unusable.\n"
                        "Rows with any errors are not present in the API but are included\n"
                        "here for transparency.\n",
                        "owner_org": "hdx-hapi",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "conflict-violence",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "title": "HDX HAPI - Coordination & Context: Conflict Events",
                    }

                    resources = dataset.get_resources()
                    assert len(resources) == 2
                    assert resources[0] == {
                        "name": "Global Coordination & Context: Conflict Events (2025)",
                        "description": "Conflict Event data from HDX HAPI (2025), "
                        "please see [the documentation](https://hdx-hapi.readthedocs.io/en/"
                        "latest/data_usage_guides/coordination_and_context/#conflict-events) "
                        "for more information",
                        "p_coded": True,
                        "format": "csv",
                    }

                    assert_files_same(
                        join(temp_folder, "hdx_hapi_conflict_event_global_2025.csv"),
                        join(fixtures_dir, "hdx_hapi_conflict_event_global_2025.csv"),
                    )
