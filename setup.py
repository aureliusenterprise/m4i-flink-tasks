from setuptools import find_packages, setup

setup(
    name="m4i_flink_tasks",
    version="1.0.1",
    author="Aurelius Enterprise",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "elasticsearch==8.3.0",
        "elastic_enterprise_search==8.3.0",
        "elastic-app-search",
        "m4i-atlas-core @ git+https://github.com/aureliusenterprise/m4i_atlas_core.git#egg=m4i-atlas-core",
        "pandas",
        "pyflink",
        "python-keycloak",
        "kafka-python==2.0.2",
        "jsonpickle",
    ],
    extras_require={
        "dev": [
            "mock",
            "pytest",
            "pytest-cov"
        ]
    }
)
