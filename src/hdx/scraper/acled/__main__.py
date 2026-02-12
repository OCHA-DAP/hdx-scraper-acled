#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os import getenv
from os.path import dirname, expanduser, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.acled.pipeline import Pipeline

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-acled"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: ACLED"


def main(
    save: bool = True,
    use_saved: bool = False,
    err_to_hdx: Optional[bool] = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (Optional[bool]): Whether to write any errors to HDX metadata.

    Returns:
        None
    """
    logger.info(f"##### {_USER_AGENT_LOOKUP} ####")
    if err_to_hdx is None:
        err_to_hdx = getenv("ERR_TO_HDX")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access("hdx-hapi", "create_dataset"):
        raise PermissionError(
            "API Token does not give access to HDX-HAPI organisation!"
        )

    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with temp_dir(folder=_USER_AGENT_LOOKUP) as temp_folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_folder,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_folder,
                    save=save,
                    use_saved=use_saved,
                )

                acled = Pipeline(configuration, retriever, temp_folder, error_handler)
                acled.get_pcodes()

                today = now_utc()
                year = today.year
                acled.download_data(year)

                dataset = acled.generate_dataset()
                dataset.update_from_yaml(
                    path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                )

    logger.info("HDX Scraper ACLED pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
