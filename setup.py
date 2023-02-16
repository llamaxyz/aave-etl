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
        "dagster-dbt",
        "dbt-bigquery",
        "boto3",  # used by Dagster Cloud Serverless
        "pandas",
        "pandas_gbq",
        "google-auth",
        "icecream", #dev debug tool
        "requests",
        "protobuf==3.20.1", 
        # "web3==6.0.0b6", # pin to 6.0.0b6 to avoid protobuf update in next release which clashes with dagster
        "web3==5.31.0",
        "subgrounds",  # API for accessing subgraphs easily
        "shroomdk",  # API for accessing shroom Flipside Crypto data tables via SQL
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
