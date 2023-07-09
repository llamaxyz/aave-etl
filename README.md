# Llama Data Warehouse

This repo contains the code for the Llama Dagster deployment at [https://llama.dagster.cloud](https://llama.dagster.cloud)

[Dagster](https://dagster.io/) is a modern data orchestration platform, available for use both as a cloud-hosted service or via the self-hosted open-source software.

## Data Sources
API keys are required from the following data providers:
- [Alchemy](https://www.alchemy.com/)
- [Covalent](https://www.covalenthq.com/)
- [Flipside Crypto](https://flipsidecrypto.xyz/)
- [Etherscan](https://etherscan.io/)
- [Polygonscan](https://polygonscan.com/)

Alchemy apps need to be set up for Ethereum, Polygon, Optimism and Arbitrum.

Alternative RPC providers can be configured using the CONFIG_CHAINS variable in `~/aave_data/resources/financials_config.py`

## Installation
This installation has been developed & tested with Python 3.10.  Python >= 3.10 is required.

To install the instance locally, clone the repo and run:

```
pip install --editable '.[dev]'
pip install multicall==0.7.1
pip install web3==6.0.0
```
_note: packages will install with version conflicts. This is required to manage incompatible dependencies between multicall, web3 and dagster.  There is no impact on functionality._

To run in local development mode:
```
dagster dev
```

## Local Configuration
This repo requires the following environment variables set:

```
WEB3_ALCHEMY_API_KEY=<your key here>
POLYGON_ALCHEMY_KEY=<your key here>
OPTIMISM_ALCHEMY_KEY=<your key here>
ARBITRUM_ALCHEMY_KEY=<your key here>
ETHERSCAN_API_KEY=<your key here>
POLYGONSCAN_API_KEY=<your key here>
FLIPSIDE_API_KEY=<your key here>
COVALENT_KEY=<your key here>
DAGSTER_HOME=/workspaces/aave-etl
```
The environment variable `DAGSTER_DEPLOYMENT` must be set to either:
- `local_filesystem` - this materialises assets to the local filesystem.  The DBT database tables are not supported in this mode.
- `local_cloud` - this materialises assets to a Google BigQuery project

# VSCode Configuration
For users with VSCode, place the following devcontainer.json into .devcontainer/ to run inside a Dev Container :
```
{
	"name": "Python 3",
	"image": "mcr.microsoft.com/devcontainers/python:0-3.10",

	"postCreateCommand": "pip3 install --upgrade pip && pip install --editable '.[dev]' && pip install multicall==0.7.1 && pip install web3==6.0.0 && git config --global user.email 'your_email@example.com' && git config --global user.name 'Your Name'",

	//Set Environment Vars
	"runArgs": ["--env-file",".devcontainer/devcontainer.env"]
}
```
Add the environment variables to .devcontainer/devcontainer.env 

Add the BigQuery service account json file to .devcontainer/ and reference it in the `creds_file` variable in `./aave_data/__init.py__`

# BigQuery Configuration
BigQuery requires:
- A project configured to hold the data (one for dev, one for prod)
- A dataset called `financials_data_lake`
- A dataset called `protocol_data_lake`
- A dataset called `warehouse`
- A dataset called `datamart`
- A service account with BigQuery Data Editor, BigQuery Job User and BigQuery Read Session User permissions on the project
- StorageAPI enabled

These details of these should be configured under the "logic for dev/prod environments" area in `./aave_data/__init.py__`. 

