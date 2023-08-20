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
        "dagster==1.4.6",
        "dagster-cloud==1.4.6",
        "dagster-gcp==0.20.6",
        "dagster-dbt==0.20.6",
        "dbt-core==1.5.1",
        "dbt-bigquery==1.5.1",
        "boto3",
        "pandas==1.5.3",
        "pandas_gbq==0.19.2",
        "google-auth==2.19.0",
        "icecream==2.1.3",
        "requests==2.29.0",
        "httpx==0.24.1",
        "subgrounds==1.6.0",
        "flipside==2.0.8",
        "scipy==1.11.1",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
