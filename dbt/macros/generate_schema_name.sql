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
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
