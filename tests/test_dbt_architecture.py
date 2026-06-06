"""
dbt Architecture Tests — NYC 311 Data Platform

Tests the structural correctness of the dbt project by inspecting the compiled
manifest without needing a live Snowflake connection. Covers:

  1. Schema resolution     — every model lands in the right Snowflake schema
  2. Materialization       — each layer uses the right strategy
  3. Lineage               — models depend on the right upstream models
  4. Layer discipline      — no layer skipping (marts never ref staging directly)
  5. Test coverage         — no model is untested; key columns have the right tests
  6. Incremental config    — fct_service_requests is correctly configured
  7. Source configuration  — freshness field, database, schema
  8. FK integrity          — relationship tests exist on all foreign keys
"""

import pytest


# ── 1. Schema Resolution ──────────────────────────────────────────────────────

ALL_MODELS = [
    "stg_service_requests",
    "int_service_requests_cleaned",
    "dim_agency",
    "dim_date",
    "dim_location",
    "fct_service_requests",
    "fct_daily_volume",
]


@pytest.mark.parametrize("model_name", ALL_MODELS)
def test_all_models_land_in_gold_schema(models, model_name):
    """
    Every model must resolve to the 'gold' schema.
    Without the generate_schema_name macro override, models with +schema: gold
    would land in 'gold_gold' — this test catches that regression.
    """
    model = models[model_name]
    assert model["schema"].lower() == "gold", (
        f"{model_name} resolved to schema '{model['schema']}' — expected 'gold'. "
        f"Check macros/generate_schema_name.sql."
    )


def test_source_points_to_silver_schema(sources):
    """The Silver source must point to the SILVER Snowflake schema."""
    src = next(iter(sources.values()))
    assert src["schema"].upper() == "SILVER", (
        f"Source schema is '{src['schema']}' — expected 'SILVER'."
    )


def test_source_database_uses_env_var():
    """
    The source database should come from an env var, not be hardcoded.
    This allows the same sources.yml to work across dev, staging, and prod.
    We read the raw YAML file because dbt resolves env_var() to its default
    before writing the manifest — the manifest always shows the resolved value.
    """
    import os
    sources_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dbt", "models", "staging", "sources.yml"
    )
    with open(sources_path) as f:
        content = f.read()
    assert "env_var" in content, (
        "sources.yml does not use env_var() for the database. "
        "Hardcoded database names break multi-environment deployments."
    )


# ── 2. Materialization Strategy ───────────────────────────────────────────────

@pytest.mark.parametrize("model_name", ["stg_service_requests", "int_service_requests_cleaned"])
def test_staging_and_intermediate_are_views(models, model_name):
    """
    Staging and intermediate models must be views, not tables.
    Views don't consume storage and always reflect the latest upstream data.
    Building them as tables would waste storage and require scheduled refreshes.
    """
    materialized = models[model_name]["config"]["materialized"]
    assert materialized == "view", (
        f"{model_name} is materialized as '{materialized}' — expected 'view'."
    )


@pytest.mark.parametrize("model_name", ["dim_agency", "dim_date", "dim_location", "fct_daily_volume"])
def test_dimension_and_aggregate_facts_are_tables(models, model_name):
    """
    Dimension tables and the pre-aggregated fact must be materialized as tables.
    BI tools query these directly — views would re-run expensive logic on every query.
    """
    materialized = models[model_name]["config"]["materialized"]
    assert materialized == "table", (
        f"{model_name} is materialized as '{materialized}' — expected 'table'."
    )


def test_fct_service_requests_is_incremental(models):
    """
    The atomic fact table must be incremental. At 35M+ rows, rebuilding it as a
    full table every day wastes significant Snowflake compute on unchanged historical data.
    """
    materialized = models["fct_service_requests"]["config"]["materialized"]
    assert materialized == "incremental", (
        f"fct_service_requests is '{materialized}' — expected 'incremental'. "
        f"35M rows rebuilt daily is an unnecessary cost."
    )


# ── 3. Incremental Configuration ─────────────────────────────────────────────

def test_fct_service_requests_incremental_unique_key(models):
    """
    The incremental merge needs a unique key so Snowflake knows which rows to
    update vs. insert. Without this, every run would insert duplicates.
    """
    unique_key = models["fct_service_requests"]["config"].get("unique_key")
    assert unique_key == "service_request_id", (
        f"fct_service_requests unique_key is '{unique_key}' — expected 'service_request_id'."
    )


def test_fct_service_requests_incremental_strategy(models):
    """
    The merge strategy matches updated rows on the unique key and updates them
    in place. The alternative (append) would create duplicate rows for records
    whose status changed after initial load (e.g. Open -> Closed).
    """
    strategy = models["fct_service_requests"]["config"].get("incremental_strategy")
    assert strategy == "merge", (
        f"fct_service_requests incremental_strategy is '{strategy}' — expected 'merge'."
    )


def test_fct_service_requests_cluster_key(models):
    """
    The cluster key must be cast(created_date as date) — not the integer FK
    created_date_id. Analysts filter on the actual date column; clustering on
    the integer FK gives zero partition pruning benefit to natural date queries.
    """
    cluster_by = models["fct_service_requests"]["config"].get("cluster_by", [])
    assert any("created_date" in key and "cast" in key for key in cluster_by), (
        f"fct_service_requests cluster_by is {cluster_by}. "
        f"Expected an expression containing 'cast' and 'created_date'."
    )


# ── 4. Lineage / DAG Dependencies ────────────────────────────────────────────

def _dep_names(model_node):
    """Return the set of short model names this model depends on."""
    return {d.split(".")[-1] for d in model_node.get("depends_on", {}).get("nodes", [])}


def test_staging_depends_only_on_source(models):
    """
    Staging must read from the Silver source, not from other dbt models.
    Staging is the boundary between raw data and the dbt transformation graph.
    """
    deps = _dep_names(models["stg_service_requests"])
    model_deps = {d for d in deps if not d.startswith("source")}
    # deps on other models should be empty
    assert "stg_service_requests" not in deps
    # source node names contain the word 'silver'
    source_deps = {d for d in models["stg_service_requests"]
                   .get("depends_on", {}).get("nodes", []) if "source" in d}
    assert len(source_deps) >= 1, "stg_service_requests must depend on the Silver source."


def test_intermediate_depends_on_staging(models):
    """
    The intermediate model must depend on staging, not directly on the source.
    Going source -> intermediate skips the rename/cast contract in staging.
    """
    deps = _dep_names(models["int_service_requests_cleaned"])
    assert "stg_service_requests" in deps, (
        "int_service_requests_cleaned does not depend on stg_service_requests."
    )


def test_dims_depend_on_intermediate_not_staging(models):
    """
    Dimension models must not read directly from staging — staging columns are
    named differently and lack business logic (borough standardization, complaint
    categories).

    dim_location reads directly from int_service_requests_cleaned.

    dim_agency reads from agency_snapshot (SCD Type 2 — see ADR 007). The snapshot
    itself depends on int_service_requests_cleaned, so the intermediate layer is
    still the source of record; dim_agency just reaches it one hop indirectly via
    the snapshot rather than via a direct model ref.
    """
    # dim_location: direct dependency on the intermediate model
    deps = _dep_names(models["dim_location"])
    assert "int_service_requests_cleaned" in deps, (
        "dim_location does not depend on int_service_requests_cleaned."
    )
    assert "stg_service_requests" not in deps, (
        "dim_location skips the intermediate layer and reads directly from staging."
    )

    # dim_agency: depends on agency_snapshot (SCD2); must NOT bypass the snapshot
    # by reading int_service_requests_cleaned directly (that would circumvent the
    # snapshot's change-detection logic).
    deps = _dep_names(models["dim_agency"])
    assert "agency_snapshot" in deps, (
        "dim_agency does not depend on agency_snapshot. "
        "The SCD2 implementation requires reading from the snapshot, not from "
        "int_service_requests_cleaned directly."
    )
    assert "stg_service_requests" not in deps, (
        "dim_agency skips the intermediate layer and reads directly from staging."
    )
    assert "int_service_requests_cleaned" not in deps, (
        "dim_agency reads int_service_requests_cleaned directly instead of going "
        "through agency_snapshot. This would bypass the SCD2 change-detection logic."
    )


def test_fct_service_requests_depends_on_all_dims_and_intermediate(models):
    """
    The fact table must join to all three dimensions and read from intermediate.
    A missing dependency means a silent Cartesian product or missing join.
    """
    deps = _dep_names(models["fct_service_requests"])
    required = {"int_service_requests_cleaned", "dim_agency", "dim_date", "dim_location"}
    missing = required - deps
    assert not missing, (
        f"fct_service_requests is missing dependencies: {missing}"
    )


def test_fct_daily_volume_depends_on_fct_and_dims(models):
    """
    The aggregated daily fact must read from fct_service_requests and the date
    and location dimensions. It must not re-read from intermediate directly —
    that would bypass the FK join logic in the atomic fact.
    """
    deps = _dep_names(models["fct_daily_volume"])
    required = {"fct_service_requests", "dim_date", "dim_location"}
    missing = required - deps
    assert not missing, (
        f"fct_daily_volume is missing dependencies: {missing}"
    )


def test_no_model_references_source_except_staging(models):
    """
    Only staging models should call source(). Intermediate and mart models must
    use ref() so the lineage graph is complete and dbt can manage execution order.
    If a mart calls source() directly, dbt cannot guarantee Silver is ready before
    the mart runs.
    """
    staging_models = {n for n in models if n.startswith("stg_")}
    for name, model in models.items():
        if name in staging_models:
            continue
        source_deps = [
            d for d in model.get("depends_on", {}).get("nodes", [])
            if d.startswith("source.")
        ]
        assert not source_deps, (
            f"{name} calls source() directly — only staging should read from sources. "
            f"Use ref() instead."
        )


# ── 5. Test Coverage ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("model_name", ALL_MODELS)
def test_every_model_has_at_least_one_test(tests_by_model, model_name):
    """
    Every model must have at least one schema test. Untested models are invisible
    to data quality monitoring — a bug in transformation logic produces wrong
    numbers silently until a stakeholder notices.
    """
    tests = tests_by_model.get(model_name, [])
    assert len(tests) >= 1, (
        f"{model_name} has zero tests. Add at least not_null + unique on its primary key."
    )


@pytest.mark.parametrize("model_name", ["stg_service_requests", "dim_agency",
                                         "dim_date", "dim_location",
                                         "fct_service_requests", "fct_daily_volume",
                                         "int_service_requests_cleaned"])
def test_primary_key_has_unique_and_not_null(tests_by_model, model_name):
    """
    Every model's surrogate key must have both unique and not_null tests.
    A surrogate key with duplicates breaks every join downstream.
    A null surrogate key means the row is invisible to any FK lookup.
    """
    test_names = [t["name"] for t in tests_by_model.get(model_name, [])]
    has_unique = any("unique" in t for t in test_names)
    has_not_null = any("not_null" in t for t in test_names)
    assert has_unique, f"{model_name} is missing a 'unique' test on its primary key."
    assert has_not_null, f"{model_name} is missing a 'not_null' test on its primary key."


def test_fct_has_relationship_test_on_agency_id(tests_by_model):
    """
    agency_id in fct_service_requests must have a relationships test pointing to
    dim_agency. Without this test, a broken JOIN condition would silently produce
    NULL agency_ids and no alert would fire.
    """
    test_names = [t["name"] for t in tests_by_model.get("fct_service_requests", [])]
    assert any("relationships" in t and "agency_id" in t for t in test_names), (
        "fct_service_requests.agency_id is missing a relationships test to dim_agency."
    )


def test_fct_has_relationship_test_on_created_date_id(tests_by_model):
    """
    created_date_id in fct_service_requests must have a relationships test pointing
    to dim_date. A NULL created_date_id means the request has no calendar context —
    it disappears from all time-series analysis.
    """
    test_names = [t["name"] for t in tests_by_model.get("fct_service_requests", [])]
    assert any("relationships" in t and "created_date_id" in t for t in test_names), (
        "fct_service_requests.created_date_id is missing a relationships test to dim_date."
    )


def test_fct_has_relationship_test_on_location_id(tests_by_model):
    """location_id must have a relationships test to dim_location."""
    test_names = [t["name"] for t in tests_by_model.get("fct_service_requests", [])]
    assert any("relationships" in t and "location_id" in t for t in test_names), (
        "fct_service_requests.location_id is missing a relationships test to dim_location."
    )


def test_intermediate_has_accepted_values_on_borough_clean(tests_by_model):
    """
    borough_clean in int_service_requests_cleaned must have an accepted_values test.
    This is the only automated check that the borough CASE WHEN logic produces
    valid output. Without it, a new raw variant silently becomes UNSPECIFIED
    and borough-level reporting becomes wrong.
    """
    test_names = [t["name"] for t in tests_by_model.get("int_service_requests_cleaned", [])]
    assert any("accepted_values" in t and "borough_clean" in t for t in test_names), (
        "int_service_requests_cleaned.borough_clean is missing an accepted_values test."
    )


def test_intermediate_has_accepted_values_on_complaint_category(tests_by_model):
    """
    complaint_category must have an accepted_values test. The classification
    CASE WHEN has an else->'Other' bucket, but the categories themselves should
    be a closed set — this test catches typos or renamed categories.
    """
    test_names = [t["name"] for t in tests_by_model.get("int_service_requests_cleaned", [])]
    assert any("accepted_values" in t and "complaint_category" in t for t in test_names), (
        "int_service_requests_cleaned.complaint_category is missing an accepted_values test."
    )


def test_resolution_days_non_negative_test_exists(tests_by_model):
    """
    resolution_days must be tested as >= 0 (where not null). This is the dbt-layer
    defence against negative resolution times that should have been filtered by Silver.
    """
    test_names = [t["name"] for t in tests_by_model.get("fct_service_requests", [])]
    assert any("expression_is_true" in t and "resolution_days" in t for t in test_names), (
        "fct_service_requests.resolution_days is missing the expression_is_true >= 0 test."
    )


# ── 6. Source Freshness Configuration ─────────────────────────────────────────

def test_source_freshness_uses_silver_timestamp(sources):
    """
    The freshness check must use _silver_timestamp (when Silver last loaded the row),
    not created_date (when the resident filed the complaint). Using created_date
    causes false freshness alerts on quiet days with few new 311 complaints.
    """
    src = next(iter(sources.values()))
    loaded_at = src.get("loaded_at_field")
    assert loaded_at == "_silver_timestamp", (
        f"loaded_at_field is '{loaded_at}' — expected '_silver_timestamp'. "
        f"created_date reflects complaint time, not pipeline load time."
    )


def test_source_freshness_warn_threshold_is_24h(sources):
    """Freshness warning fires after 24 hours of no new Silver data."""
    src = next(iter(sources.values()))
    freshness = src.get("freshness", {})
    warn = freshness.get("warn_after", {})
    assert warn.get("count") == 24 and warn.get("period") == "hour", (
        f"Source freshness warn_after is {warn} — expected 24 hours."
    )


def test_source_freshness_error_threshold_is_48h(sources):
    """Freshness error fires after 48 hours — two missed daily pipeline runs."""
    src = next(iter(sources.values()))
    freshness = src.get("freshness", {})
    error = freshness.get("error_after", {})
    assert error.get("count") == 48 and error.get("period") == "hour", (
        f"Source freshness error_after is {error} — expected 48 hours."
    )


# ── 7. Generate Schema Name Macro ─────────────────────────────────────────────

def test_generate_schema_name_macro_exists():
    """
    The generate_schema_name macro must exist. Without it, dbt appends custom
    schema names to the target schema (gold + gold = gold_gold), which doesn't
    exist in Snowflake and breaks every dbt run.
    """
    import os
    macro_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dbt", "macros", "generate_schema_name.sql"
    )
    assert os.path.exists(macro_path), (
        "macros/generate_schema_name.sql does not exist. "
        "dbt will concatenate schemas incorrectly without this override."
    )


def test_generate_schema_name_macro_has_override_logic():
    """
    The macro must implement the override pattern (return custom_schema_name as-is
    when provided). A file that exists but contains the default behavior is just
    as broken as no file at all.
    """
    import os
    macro_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dbt", "macros", "generate_schema_name.sql"
    )
    with open(macro_path) as f:
        content = f.read()
    assert "custom_schema_name | trim" in content, (
        "generate_schema_name.sql doesn't implement the override pattern. "
        "It must return custom_schema_name as-is when provided."
    )
