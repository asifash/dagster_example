from setuptools import find_packages, setup

setup(
    name="dagster_example_pipeline",
    packages=find_packages(exclude=["dagster_example_pipeline_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
