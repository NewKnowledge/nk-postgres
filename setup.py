from setuptools import setup


setup(
    name="nk_postgres",
    version="0.2.2",
    description="Utilities for managing connections and making queries against a PostgreSQL database using psycopg2.",
    packages=["nk_postgres"],
    include_package_data=True,
    install_requires=["sqlalchemy", "psycopg2-binary", "retrying"],
)
