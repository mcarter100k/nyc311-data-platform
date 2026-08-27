{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
    Override dbt's default schema name generation.

    Default behavior appends the custom schema to the target schema:
        target.schema = "gold"  +  custom_schema = "gold"  →  "gold_gold"

    This override uses the custom schema as-is, falling back to target.schema
    when no custom schema is configured.

        target.schema = "gold"  +  no custom schema  →  "gold"   (staging, intermediate views)
        target.schema = "gold"  +  custom_schema = "gold"  →  "gold"  (marts)

    Result: all dbt models land in the Snowflake GOLD schema, which is the schema
    provisioned by Terraform and granted to the TRANSFORMER role. Staging and
    intermediate models are views and co-locate with mart tables without ambiguity.

    To separate staging/intermediate into their own Snowflake schemas in the future:
      1. Provision STAGING and INTERMEDIATE schemas in terraform/modules/snowflake-foundation/main.tf
      2. Grant TRANSFORMER USAGE + CREATE TABLE/VIEW on both schemas
      3. Re-enable +schema: staging and +schema: intermediate in dbt_project.yml
      4. This macro will then route each layer to its dedicated schema.

    Write-audit-publish: when the `audit_suffix` var is set, model schemas are
    suffixed so the whole run builds and tests in GOLD_AUDIT instead of GOLD,
    and the publish_gold run-operation swaps the audited schema into place.
    Snapshots are never suffixed — SCD2 state must live in exactly one schema
    across audit and production runs.

    NOTHING IN THIS REPOSITORY SETS THAT VAR. The mechanism is available but
    unused: `var('audit_suffix', '')` defaults to empty, so every run this repo
    performs lands directly in GOLD. To use it you pass the var yourself:

        dbt build --vars '{"audit_suffix": "_audit"}'
        dbt run-operation publish_gold

    This comment previously read "(Airflow passes '_audit')", which was false.
    The Airflow DAG's `dbt_build` task runs `local/local_runner.py --only 4`
    against local DuckDB, and the local mirror of this macro has no
    audit_suffix branch at all. No workflow, script, or dbt_project.yml var
    passes it either. Wiring it into the DAG would not fix the comment, it
    would break the pipeline: publish_gold's swap is `ALTER SCHEMA ... SWAP
    WITH`, a Snowflake operation with no DuckDB equivalent, so the local run
    would build into a suffixed schema that nothing ever publishes. See
    docs/CLAIMS.md, which has recorded the accurate version of this all along.
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {%- set base_schema = default_schema -%}
    {%- else -%}
        {%- set base_schema = custom_schema_name | trim -%}
    {%- endif -%}
    {%- if node.resource_type == 'snapshot' -%}
        {{ base_schema }}
    {%- else -%}
        {{ base_schema ~ var('audit_suffix', '') }}
    {%- endif -%}
{%- endmacro %}
