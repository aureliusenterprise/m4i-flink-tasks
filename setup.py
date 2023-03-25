from setuptools import find_packages, setup
import os

M4I_FLINK_ATLAS_CORE_BRANCH = os.getenv('M4I_FLINK_ATLAS_CORE_BRANCH')
M4I_FLINK_ATLAS_CORE_TAG = os.getenv('M4I_FLINK_ATLAS_CORE_TAG')
atlas_core_link = "m4i-atlas-core @ git+https://github.com/aureliusenterprise/m4i_atlas_core.git"
if M4I_FLINK_ATLAS_CORE_BRANCH:
    atlas_core_link = atlas_core_link+"@"+M4I_FLINK_ATLAS_CORE_BRANCH
atlas_core_link = atlas_core_link+"#egg=m4i-atlas-core"
if M4I_FLINK_ATLAS_CORE_TAG:
    atlas_core_link = atlas_core_link+"-"+M4I_FLINK_ATLAS_CORE_TAG

setup(
    name="m4i_flink_tasks",
    version="1.0.1",
    author="Aurelius Enterprise",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        # "avro",
        # "dict_hash",
        "elasticsearch==8.3.0",
        "elastic_enterprise_search==8.3.0",
        "elastic-app-search",
        atlas_core_link,
        #"m4i-atlas-core @ git+https://github.com/aureliusenterprise/m4i_atlas_core.git#egg=m4i-atlas-core",
        #"m4i-data-management @ git+https://github.com/wombach/m4i-data-management.git#egg=m4i-data-management",
        # "fastavro",
        # "jsonschema",
        "pandas",
        "pytest",
        "python-keycloak",
        "kafka-python==2.0.2",
        "jsonpickle",
        "pylzma",
        "python-xz",
        # "requests",
        # "typing_extensions",
        # "deepdiff",
        # "python-dotenv",
    ],
    extras_require={
        "dev": [
            "mock",
            "pytest",
            "pytest-cov"
        ]
    }
)
