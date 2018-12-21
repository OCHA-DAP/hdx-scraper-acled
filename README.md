### Collector for ACLED's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-acled.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-acled) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-acled/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-acled?branch=master)

This script connects to the [ACLED API](https://www.acleddata.com/download/2833/) and extracts data country by country creating a dataset per country in HDX. The scraper takes around 20 minutes to run. It makes in the order of 200 reads from ACLED and 1000 read/writes (API calls) to HDX in total. It does not create temporary files as it puts urls into HDX. It is run when ACLED make changes (not in their data but for example in their API), in practice this is in the order of once or twice a year. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-acled** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY