from setuptools import setup, find_packages

setup(
    name="modern-data-lakehouse",
    version="1.0.0",
    description="PySpark-based ETL pipeline for a Modern Data Lakehouse (Snowflake + BigQuery)",
    author="Your Name",
    author_email="you@example.com",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests*", "notebooks*", "docs*"]),
    install_requires=[
        "pyspark>=3.5.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "snowflake": ["snowflake-connector-python[pandas]>=3.7.0"],
        "bigquery": ["google-cloud-bigquery>=3.17.0", "google-cloud-bigquery-storage>=2.24.0"],
        "airflow": ["apache-airflow>=2.9.0"],
        "dev": ["pytest", "pytest-cov", "black", "ruff", "mypy", "chispa"],
    },
    entry_points={
        "console_scripts": [
            "lakehouse-run=etl.pipeline:main",
        ],
    },
)
