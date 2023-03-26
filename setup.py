from setuptools import find_packages, setup

setup(
    name="m4i_flink_tasks",
    version="1.0.1",
    author="Aurelius Enterprise",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "apache-flink",
        "elasticsearch",
        "elastic_enterprise_search",
        "elastic-app-search",
        "jsonpickle",
        "kafka-python",
        "m4i-atlas-core @ git+https://github.com/aureliusenterprise/m4i_atlas_core.git#egg=m4i-atlas-core",
        "numpy",
        "pandas",
        "python-keycloak"
    ],
    extras_require={
        "dev": [
            "mock",
            "pytest",
            "pytest-asyncio",
            "pytest-cov"
        ]
    }
)
