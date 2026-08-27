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
    "stg_data_quality_log",
    "stg_quarantine",
    "int_service_requests_cleaned",
    "int_load_completeness",
    "dim_agency",
    "dim_date",
    "dim_location",
    "fct_service_requests",
    "fct_daily_volume",
    "fct_data_quality",
    "fct_complaint_recurrence",
]


def test_all_models_list_matches_manifest(models):
    """
    ALL_MODELS must equal the manifest's model set exactly. Without this sync
    check the list drifts silently: a model added to the project but not to
    the list escapes every parametrized test below — which happened twice
    (stg_data_quality_log, then fct_data_quality) before this test existed.
    """
    assert sorted(ALL_MODELS) == sorted(models.keys()), (
        f"ALL_MODELS is out of sync with the compiled manifest.\n"
        f"  missing from list: {sorted(set(models.keys()) - set(ALL_MODELS))}\n"
        f"  stale in list:     {sorted(set(ALL_MODELS) - set(models.keys()))}"
    )


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

@pytest.mark.parametrize("model_name", ["stg_service_requests", "stg_data_quality_log",
                                       "stg_quarantine"])
def test_staging_is_view(models, model_name):
    """
    Staging must be a view, not a table. It is a thin rename/cast layer read by
    exactly one downstream model — a view costs no storage and always reflects
    the latest Silver data.
    """
    materialized = models[model_name]["config"]["materialized"]
    assert materialized == "view", (
        f"{model_name} is materialized as '{materialized}' — expected 'view'."
    )


def test_intermediate_is_table(models):
    """
    int_service_requests_cleaned must be a table, not a view. Three models read
    it (dim_location, agency_snapshot, fct_service_requests) — as a view, its
    12-branch leading-wildcard ILIKE classification would re-execute over full
    history three times per run. Materializing computes it once.
    """
    materialized = models["int_service_requests_cleaned"]["config"]["materialized"]
    assert materialized == "table", (
        f"int_service_requests_cleaned is materialized as '{materialized}' — expected 'table'."
    )


@pytest.mark.parametrize("model_name", ["dim_agency", "dim_date",
                                         "fct_daily_volume", "fct_data_quality",
                                         "fct_complaint_recurrence"])
def test_dimension_and_aggregate_facts_are_tables(models, model_name):
    """
    Dimension tables and the derived facts must be materialized as tables.
    BI tools query these directly — views would re-run expensive logic on every query.

    dim_location is deliberately absent: it is incremental for retention rather
    than for cost (see test_dim_location_is_incremental). dim_agency stays a
    table because its retention comes from the snapshot feeding it, which never
    deletes rows, so rebuilding the table loses nothing.
    """
    materialized = models[model_name]["config"]["materialized"]
    assert materialized == "table", (
        f"{model_name} is materialized as '{materialized}' — expected 'table'."
    )


def test_dim_location_is_incremental(models):
    """
    dim_location must be incremental — a RETENTION requirement, not a cost one.

    Rebuilt as a table it was reconstructed each run from
    int_service_requests_cleaned, which carries only Silver's rolling window,
    while fct_service_requests accumulates history far past that window. Every
    location that stopped appearing in the window was dropped from the
    dimension while fact rows kept pointing at its location_id, so the FK
    silently dangled and fct_daily_volume reattributed the volume to borough
    'UNSPECIFIED'. A Kimball dimension grows and never loses members.
    """
    materialized = models["dim_location"]["config"]["materialized"]
    assert materialized == "incremental", (
        f"dim_location is '{materialized}' — expected 'incremental'. Rebuilding it "
        f"drops every location that has aged out of Silver's rolling window and "
        f"dangles the location_id of every accumulated fact row pointing at it."
    )


def test_dim_location_incremental_unique_key(models):
    """
    The accumulation is keyed on location_id, so a member observed again in a
    later window is upserted rather than duplicated. Without a unique key the
    strategy degrades to append and the dimension grows a duplicate row per run
    per location, breaking its `unique` test and fanning out every join to it.
    """
    unique_key = models["dim_location"]["config"].get("unique_key")
    assert unique_key == "location_id", (
        f"dim_location unique_key is '{unique_key}' — expected 'location_id'."
    )


def test_dim_location_incremental_strategy(models):
    """
    merge on Snowflake, matching fct_service_requests. The local DuckDB mirror
    uses delete+insert (dbt-duckdb implements no merge); that divergence is
    registered in scripts/model_drift_baseline.json.
    """
    strategy = models["dim_location"]["config"].get("incremental_strategy")
    assert strategy == "merge", (
        f"dim_location incremental_strategy is '{strategy}' — expected 'merge'."
    )


def test_fct_service_requests_is_incremental(models):
    """
    The atomic fact table must be incremental. At ~22M rows, rebuilding it as a
    full table every day wastes significant Snowflake compute on unchanged historical data.
    """
    materialized = models["fct_service_requests"]["config"]["materialized"]
    assert materialized == "incremental", (
        f"fct_service_requests is '{materialized}' — expected 'incremental'. "
        f"~22M rows rebuilt daily is an unnecessary cost."
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

    Classified on FULL manifest node ids, not the short names `_dep_names`
    returns. A dbt source id looks like `source.nyc311.silver.service_requests`
    and a model id like `model.nyc311.int_service_requests_cleaned`; shortening
    them to the last segment discards exactly the prefix that tells the two
    apart. The earlier version of this test shortened first and then filtered on
    `not startswith("source")`, which no short name can ever satisfy — so its
    `model_deps` set was unconditionally non-empty, and the assertion that would
    have used it was never written. It asserted only that the model does not
    depend on itself, which is trivially true for any staging model however
    wired, and the test passed while checking nothing.
    """
    nodes = models["stg_service_requests"].get("depends_on", {}).get("nodes", [])
    model_deps = sorted(n for n in nodes if n.startswith("model."))
    source_deps = sorted(n for n in nodes if n.startswith("source."))

    assert not model_deps, (
        f"stg_service_requests must read the Silver source only, but depends on "
        f"model(s): {model_deps}. Staging is the boundary between raw data and "
        f"the dbt graph; a model dependency here moves that boundary."
    )
    assert len(source_deps) >= 1, (
        "stg_service_requests must depend on the Silver source; its depends_on "
        f"lists no source node (got: {nodes})."
    )


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


def test_recurrence_horizon_comes_from_load_completeness(models):
    """
    fct_complaint_recurrence must take its observation horizon from
    int_load_completeness.

    This pins the fix for a defect that reads as obviously correct in the SQL:
    the horizon used to be max(created_date) over the load. The source publishes
    on a ~23.5h lag, so the newest created_date is never a whole day — it is the
    first couple of hours of one — and every observation_days value was inflated
    by up to a full day against a horizon that did not exist yet. A future edit
    that drops this dependency has almost certainly re-derived the horizon
    locally, which is the shape of the original bug.
    """
    deps = _dep_names(models["fct_complaint_recurrence"])
    assert "int_load_completeness" in deps, (
        "fct_complaint_recurrence no longer depends on int_load_completeness — "
        "its observation horizon is being derived somewhere else. The last "
        "COMPLETE load day is the only honest horizon; the newest loaded day is "
        "always partial."
    )


def test_daily_volume_carries_load_completeness(models):
    """
    fct_daily_volume must also read int_load_completeness. Every figure on that
    table is a per-day figure, and the newest loaded day is a ~2-hour day; a
    daily mean taken across it without the flag is contaminated the same way the
    recurrence horizon was. The point of the shared model is that both consumers
    read ONE definition — this asserts the second one still does.
    """
    deps = _dep_names(models["fct_daily_volume"])
    assert "int_load_completeness" in deps, (
        "fct_daily_volume no longer depends on int_load_completeness — its "
        "is_complete_day flag is gone or locally re-derived, and per-day "
        "averages across a partial day are back."
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


# ── Primary key coverage ──────────────────────────────────────────────────────
#
# The declared grain of every model: the column, or tuple of columns, whose
# uniqueness the model guarantees. This is the thing the tests below verify is
# actually asserted in the warehouse — so it has to be written down. Reading it
# out of test NAMES is what made the previous version of this guard inert.
#
# Every model in the manifest must appear here or in PRIMARY_KEY_EXEMPTIONS;
# test_every_model_declares_a_primary_key_or_an_exemption enforces that, so a
# new model cannot slip past this section by simply not being listed.
PRIMARY_KEYS = {
    "stg_service_requests": ("service_request_id",),
    "stg_data_quality_log": ("run_date", "check_name"),
    "stg_quarantine": ("unique_key",),
    "int_load_completeness": ("load_day",),
    "dim_agency": ("agency_key",),
    "dim_date": ("date_id",),
    "dim_location": ("location_id",),
    "fct_service_requests": ("service_request_id",),
    "fct_daily_volume": ("daily_volume_id",),
    "fct_data_quality": ("run_date", "check_name"),
    "fct_complaint_recurrence": ("service_request_id",),
}

# Exemptions carry their REASON, not just their name. An exemption with no
# stated reason is indistinguishable from an oversight six months later, and a
# bare list of names is how exemptions accumulate silently — each new one added
# because the list already had entries.
PRIMARY_KEY_EXEMPTIONS = {
    "int_service_requests_cleaned": (
        "Uniqueness is asserted at the boundary (stg_service_requests.unique_key) "
        "and on the published artifact (fct_service_requests.service_request_id). "
        "This layer is a row-preserving projection — it adds derived columns and "
        "never joins or unions — so it cannot introduce duplicates, and testing "
        "it costs two extra full-table scans per dbt test run for no new signal."
    ),
}


def _test_type(test_node):
    """The dbt test TYPE ('unique', 'not_null', 'relationships', ...).

    This is the field the previous version of this guard should have read.
    Generic tests carry test_metadata.name; singular tests (the hand-written
    .sql files under dbt/tests/) carry no test_metadata at all and return None.
    """
    return (test_node.get("test_metadata") or {}).get("name")


def _tested_columns(test_node):
    """The set of columns a test node covers, lowercased.

    Single-column generic tests set column_name. dbt_utils.unique_combination_
    of_columns sets column_name to null and lists its columns in
    test_metadata.kwargs.combination_of_columns.
    """
    column = test_node.get("column_name")
    if column:
        return {column.lower()}
    kwargs = (test_node.get("test_metadata") or {}).get("kwargs") or {}
    combination = kwargs.get("combination_of_columns") or []
    return {c.lower() for c in combination}


def _asserts_uniqueness_of(test_node, key_columns):
    """Does this test node prove `key_columns` is unique?

    Two legitimate shapes, both accepted:

      unique                          on a column of the key
      unique_combination_of_columns   over the key's columns

    Subset, not equality, and that is deliberate rather than sloppy: uniqueness
    of any SUBSET of a key implies uniqueness of the whole key. A `unique` test
    on run_date alone is a strictly stronger claim than one on
    (run_date, check_name), so refusing it would be a false negative. The
    subset must be non-empty, which is what stops a singular test (no columns
    at all) from matching vacuously.
    """
    if _test_type(test_node) not in ("unique", "unique_combination_of_columns"):
        return False
    covered = _tested_columns(test_node)
    return bool(covered) and covered <= key_columns


def test_every_model_declares_a_primary_key_or_an_exemption(models):
    """
    PRIMARY_KEYS + PRIMARY_KEY_EXEMPTIONS must cover the manifest exactly.

    Without this, the key guard below is scoped by a hand-written list and a new
    model escapes it by never being added — which is precisely how the previous
    six-model list came to omit five of the twelve models in the project.
    """
    declared = set(PRIMARY_KEYS)
    exempt = set(PRIMARY_KEY_EXEMPTIONS)
    actual = set(models.keys())

    overlap = declared & exempt
    assert not overlap, (
        f"Model(s) both declare a primary key and claim an exemption: {sorted(overlap)}. "
        f"Pick one."
    )
    assert declared | exempt == actual, (
        f"Primary-key coverage is out of sync with the compiled manifest.\n"
        f"  no key declared and not exempt: {sorted(actual - declared - exempt)}\n"
        f"  listed but not a model:         {sorted((declared | exempt) - actual)}"
    )
    for name, reason in PRIMARY_KEY_EXEMPTIONS.items():
        assert len(reason.strip()) >= 40, (
            f"The primary-key exemption for {name} states no substantive reason. "
            f"An exemption without one is indistinguishable from an oversight."
        )


@pytest.mark.parametrize("model_name", sorted(PRIMARY_KEYS))
def test_primary_key_has_unique_and_not_null(tests_by_model, model_name):
    """
    Every model's declared primary key must be proven unique AND not null.
    A key with duplicates breaks every join downstream. A null key means the row
    is invisible to any FK lookup.

    Matched on the manifest's STRUCTURED test data — test_metadata.name for the
    test's type, column_name / kwargs for the columns it covers — not on
    substrings of a generated test name. Name matching is why this guard could
    not fail: `any(name.startswith("unique_"))` is satisfied by the unique test
    on fct_service_requests.UNIQUE_KEY, so deleting the real uniqueness test on
    service_request_id left the guard green (verified by mutation). The same
    held for dim_date, whose PK date_id was shadowed by the unique test on
    full_date, and for every not_null check on this list — dim_location has
    three other not_null tests, any one of which satisfied a check that never
    looked at which column it named.
    """
    key = set(PRIMARY_KEYS[model_name])
    tests = tests_by_model.get(model_name, [])

    unique_tests = [t for t in tests if _asserts_uniqueness_of(t, key)]
    assert unique_tests, (
        f"{model_name}: nothing in the manifest asserts that its primary key "
        f"({', '.join(sorted(key))}) is unique.\n"
        f"  uniqueness tests on this model cover: "
        f"{sorted(sorted(_tested_columns(t)) for t in tests if _test_type(t) in ('unique', 'unique_combination_of_columns')) or 'nothing'}\n"
        f"  A unique test on a DIFFERENT column does not make the key unique."
    )

    not_null_columns = set()
    for t in tests:
        if _test_type(t) == "not_null":
            not_null_columns |= _tested_columns(t)
    missing = sorted(key - not_null_columns)
    assert not missing, (
        f"{model_name}: primary key column(s) {missing} have no not_null test. "
        f"A null key column makes the row invisible to every FK lookup into "
        f"this model.\n"
        f"  not_null tests on this model cover: {sorted(not_null_columns) or 'nothing'}"
    )


# agency_id and location_id used to be excused from relationships tests on the
# argument that dim_agency and dim_location are derived from
# int_service_requests_cleaned — the same source as the fact — so their FKs
# resolved by construction and the tests could never fail.
#
# That argument held only while Gold was rebuilt from scratch every run. It
# stopped being true when fct_service_requests became incremental: the fact
# accumulates history, dim_location was rebuilt from Silver's rolling window,
# and every location aging out of the window was dropped while fact rows kept
# pointing at it (88 dangling rows on the production artifact, growing daily,
# and nothing noticed because the excused test was the only thing that would
# have looked). Shared lineage is not a referential-integrity guarantee; equal
# RETENTION is. Every FK on the fact now carries a relationships test.
@pytest.mark.parametrize("column,dimension", [
    ("created_date_id", "dim_date"),
    ("location_id", "dim_location"),
    ("agency_id", "dim_agency"),
])
def test_fct_has_relationship_test_on_every_foreign_key(tests_by_model, models,
                                                        column, dimension):
    """
    Every foreign key on fct_service_requests must carry a relationships test to
    its dimension. A dangling FK is not a loud failure — the row survives, the
    join just returns nothing, and the measure quietly lands in whatever bucket
    the consuming model coalesces to.
    """
    tests = tests_by_model.get("fct_service_requests", [])
    matching = [
        t for t in tests
        if t["name"].startswith("relationships_") and column in t["name"]
    ]
    assert matching, (
        f"fct_service_requests.{column} is missing a relationships test to {dimension}."
    )
    # Name-matching alone would pass a test pointed at the wrong dimension.
    # Assert the real edge in the manifest instead.
    dimension_id = models[dimension]["unique_id"]
    assert any(dimension_id in t["depends_on"]["nodes"] for t in matching), (
        f"fct_service_requests.{column} has a relationships test, but it does not "
        f"reference {dimension} — the FK is being checked against the wrong table."
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
