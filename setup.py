from setuptools import find_packages, setup
from setuptools.command.develop import develop
from setuptools.command.install import install
from subprocess import check_call


# setup(
#     name="quickstart_gcp",
#     packages=find_packages(exclude=["quickstart_gcp_tests"]),
#     install_requires=[
#         "dagster",
#         "dagster-gcp",
#         "dagster-cloud",
#         "boto3",  # used by Dagster Cloud Serverless
#         "pandas",
#         "matplotlib",
#         "textblob",
#         "tweepy",
#         "wordcloud",
#         "pandas_gbq",
#         "google-auth",
#     ],
#     extras_require={"dev": ["dagit", "pytest"]},
# )

setup(
    name="financials",
    packages=find_packages(exclude=["quickstart_gcp_tests","financial_tests"]),
    install_requires=[
        "dagster",
        "dagster-gcp",
        "dagster-cloud",
        "boto3",  # used by Dagster Cloud Serverless
        "pandas",
        "pandas_gbq",
        "google-auth",
        "icecream", #dev debug tool
        "requests",
        "git", # required for installing from git repos
        # "subgrounds",  # API for accessing subgraphs easily
        # "eth-ape==0.5.4", 
        # custom version of ape used to work around protobuf dependencies, relax after https://github.com/dagster-io/dagster/issues/10627 is resolved
        # "eth-ape @ git+https://github.com/scottincrypto/ape.git@pin_protobuf_3.20.1",
        # "ape-alchemy",
        # "ape-arbitrum",
        # "ape-etherscan",
        # "ape-fantom",
        # "ape-optimism",
        # "ape-polygon",
        # "ape-harmony @ git+https://github.com/scottincrypto/ape-harmony.git",
        # "ape-avalanche @ git+https://github.com/scottincrypto/ape-avalanche.git@update-for-ape-0.5",
        # "protobuf==4.21.12",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
