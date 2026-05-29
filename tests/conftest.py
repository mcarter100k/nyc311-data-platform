"""
Shared fixtures for the NYC 311 pipeline test suite.

These fixtures load the dbt manifest (the compiled project graph) and make it
available to every test. The manifest is the single source of truth for what
dbt knows about the project — schemas, dependencies, tests, configs.
"""

import json
import os
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MANIFEST_PATH = os.path.join(ROOT, "dbt", "target", "manifest.json")


@pytest.fixture(scope="session")
def manifest():
    with open(MANIFEST_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def models(manifest):
    """All dbt model nodes keyed by short model name."""
    return {
        node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    }


@pytest.fixture(scope="session")
def dbt_tests(manifest):
    """All dbt test nodes as a list."""
    return [
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "test"
    ]


@pytest.fixture(scope="session")
def tests_by_model(manifest, dbt_tests):
    """Map of model_name -> list of test nodes attached to that model."""
    result = {}
    for test in dbt_tests:
        attached = test.get("attached_node", "")
        model_name = attached.split(".")[-1] if attached else None
        if model_name:
            result.setdefault(model_name, []).append(test)
    return result


@pytest.fixture(scope="session")
def sources(manifest):
    """All source nodes keyed by source identifier."""
    return manifest.get("sources", {})
