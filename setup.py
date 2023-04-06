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
    name="aave_data",
    packages=find_packages(exclude=["quickstart_gcp_tests","aave_data_tests"]),
    install_requires=[
        "dagster==1.2.6",
        "dagster-gcp==0.18.6",
        "dagster-cloud==1.2.6",
        "dagster-dbt==0.18.6",
        "dbt-bigquery",
        "boto3",  # used by Dagster Cloud Serverless
        "pandas",
        "pandas_gbq",
        "google-auth",
        "icecream", #dev debug tool
        "requests",
        "subgrounds",  # API for accessing subgraphs easily
        "shroomdk",  # API for accessing shroom Flipside Crypto data tables via SQL
        # "web3==6.0.0", # installed in dagster_cloud_post_install.sh due to version clash with multicall
        # "multicall==0.7.1" # installed in dagster_cloud_post_install.sh due to version clash with web3
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
