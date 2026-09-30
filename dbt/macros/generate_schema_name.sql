{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
    Use the configured schema as-is instead of dbt's default <target>_<custom>,
    so marts (+schema: gold) land in GOLD rather than GOLD_GOLD. Staging views
    and intermediate tables have no custom schema and land in target.schema,
    which is also GOLD.

    Write-audit-publish (Snowflake only): passing the `audit_suffix` var builds
    and tests every model in GOLD_AUDIT, and publish_gold then swaps it into
    GOLD. Nothing in this repo sets audit_suffix; run the two commands by hand:

        dbt build --vars '{"audit_suffix": "_audit"}'
        dbt run-operation publish_gold

    Snapshots set target_schema, so dbt never routes them through this macro;
    they stay in SNAPSHOTS.
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {%- set base_schema = default_schema -%}
    {%- else -%}
        {%- set base_schema = custom_schema_name | trim -%}
    {%- endif -%}
    {{ base_schema ~ var('audit_suffix', '') }}
{%- endmacro %}
