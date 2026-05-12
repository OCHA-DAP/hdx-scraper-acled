# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-acled** collects conflict events data from [ACLED](https://acleddata.com/) via HDX datasets and produces a unified global conflict events dataset for [HDX HAPI](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/coordination_and_context/#conflict-events). It is updated weekly.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.acled
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_acled.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through three stages in `__main__.py`:

1. **`get_pcodes`** — Downloads admin level 1 and 2 p-code reference data using `AdminLevel` from `hdx-python-country`.

2. **`download_data`** — For each configured ACLED HDX dataset, reads the Excel file, normalises columns, resolves ISO3 country codes and p-codes, and splits rows by year into `self.data`.

3. **`generate_dataset`** — Constructs a single global HDX `Dataset` with one CSV resource per year.

### Key design points

- **One global dataset, one resource per year**: unlike per-country scrapers, ACLED produces a single `hdx-hapi-conflict-event` dataset with yearly CSV resources.
- **`Retrieve`** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` — used in tests to replay fixture data from `tests/fixtures/input/`.
- **Static config inside the package**: `config/` lives under `src/hdx/scraper/acled/config/` so it is installed with the package and located via `script_dir_plus_file`.

### Config files

- `src/hdx/scraper/acled/config/project_configuration.yaml` — Dataset names, sheet names, column headers, resource naming templates
- `src/hdx/scraper/acled/config/hdx_dataset_static.yaml` — Static HDX metadata applied to the dataset (license, methodology, source, etc.)

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-acled` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
