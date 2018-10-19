from setuptools import setup


setup(
    name="nk_postgres",
    version="1.0.0",
    description="Utilities for managing connections and making queries against a PostgreSQL database using psycopg2.",
    packages=["nk_postgres"],
    include_package_data=True,
    install_requires=["psycopg2-binary", "retrying"],
)