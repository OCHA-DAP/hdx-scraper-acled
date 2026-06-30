# Collector for ACLED Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-acled/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-acled/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-acled/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-acled?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline reads conflict events data from three [ACLED](https://acleddata.com/) datasets on HDX and produces a unified global conflict events dataset for [HDX HAPI](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/coordination_and_context/#conflict-events). It makes approximately 8 reads from HDX (2 p-code reference files and 3 metadata reads + 3 Excel downloads for the ACLED source datasets) and 1 write to create or update the output dataset. It downloads 3 Excel files (several MB each) and generates ~28–30 yearly CSV files (~tens of MB total) in a temporary directory before uploading them as resources. For each row, the event type is derived from the source dataset name, ISO3 codes are resolved from an explicit column or fuzzy-matched from the country name, p-codes are validated against a global registry, HRP/GHO status is looked up per country, and reference period dates are constructed from month/year values. It runs weekly on Fridays at around 9 AM UTC and takes approximately 30 minutes to complete.

## Data Pipeline

### API reads (~8 calls per run)

- **P-code reference files** (2 downloads, ~1.4 MB total): a global admin1/admin2 p-code registry CSV (~1.4 MB) and a p-code format/length definitions CSV (~2 KB), both fetched from the HDX Tools GitHub repository.
- **ACLED source datasets** (3 metadata reads + 3 Excel downloads): one HDX dataset metadata read and one Excel file download per source dataset (`civilian-targeting-events-and-fatalities`, `demonstration-events`, `political-violence-events-and-fatalities`). Each Excel file is several MB and contains three sheets: `Non_HRP`, `HRP_1`, and `HRP_2`.

### API writes (~1 call per run)

- **HDX dataset update** (1 write): the `hdx-hapi-conflict-event` dataset is created or updated with approximately 28–30 CSV resources, one per calendar year covered in the source data (1997 to the current year).

### Temporary files

- 3 downloaded ACLED Excel files: several MB each (~tens of MB total).
- ~28–30 generated CSV files (one per year): a few MB each (~tens of MB total), written to a temporary directory before upload.

### Uploaded files

- ~28–30 CSV resources on HDX, each containing all conflict event rows for one calendar year. Each CSV has 19 columns and the row count grows with years of coverage and the number of HRP countries tracked at admin level 2.

### Transformations

The pipeline processes each of the 3 ACLED source datasets, reading all 3 sheets per file (9 sheets total):

1. **Event type**: derived from the HDX dataset name by stripping the trailing `-events[-and-fatalities]` suffix and replacing hyphens with underscores (e.g. `civilian-targeting-events-and-fatalities` → `civilian_targeting`).

2. **Admin level detection**: sheets with no `Admin1`/`Admin2` columns (`Non_HRP`) are assigned `admin_level = 0` (country-only); sheets with those columns (`HRP_1`, `HRP_2`) are assigned `admin_level = 2`.

3. **Duplicate detection**: rows are checked for duplicates on the combination of admin2 p-code (or country name for admin_level 0), admin names, event type, month, and year. Duplicates are flagged with an `error` field and reported to the HDX error handler.

4. **ISO3 resolution**: taken directly from the `ISO3` column when present (HRP sheets); otherwise fuzzy-matched from the `Country` name. Kosovo is special-cased to `XKX` regardless.

5. **HRP/GHO status**: `has_hrp` (Y/N) and `in_gho` (Y/N) are looked up per ISO3 code from the `hdx-python-country` library.

6. **Reference period**: `Month` + `Year` values are parsed into ISO 8601 date strings covering the full calendar month (`reference_period_start`, `reference_period_end`).

7. **P-code validation** (admin_level 2 only): admin1 and admin2 p-codes and provider names are passed to `complete_admins()` from `hdx-pipelineutils`, which cross-references the global p-code registry to validate codes, fill in canonical admin names, and record any mismatches as warnings.

8. **Column renaming**: source columns are renamed to the HAPI output schema (`ISO3` → `location_code`, `Admin1` → `provider_admin1_name`, `Admin2` → `provider_admin2_name`, `Fatalities` → `fatalities`, `Events` → `events`).

9. **Provenance**: `dataset_hdx_id` and `resource_hdx_id` (from the source ACLED HDX dataset/resource) are added to every row.

10. **Year partitioning**: all rows are partitioned by `Year`. Data from all 9 sheets is concatenated per year and written to a separate CSV, which is uploaded as an individual HDX resource.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-acled** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.acled
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
