{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
    Use the configured schema as-is instead of dbt's default <target>_<custom>,
    so marts (+schema: gold) land in GOLD rather than GOLD_GOLD. Staging views
    and intermediate tables have no custom schema and land in target.schema,
    which is also GOLD.

    Write-audit-publish: the `audit_suffix` var builds and tests every model in
    GOLD_AUDIT. On this DuckDB path, local/wap.py passes it and then copies the
    audited build into GOLD in one transaction (ADR 018).

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
