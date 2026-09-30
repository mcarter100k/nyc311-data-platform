{% macro publish_gold(audit_suffix='_audit') %}
{#
    Publish step of write-audit-publish, for a Snowflake target. Nothing in this
    repo calls it; run it by hand after an audited build:

        dbt build --vars '{"audit_suffix": "_audit"}'   # build + test in GOLD_AUDIT
        dbt run-operation publish_gold                  # atomic swap into GOLD

    If the build or any test fails, GOLD is untouched. After a swap GOLD_AUDIT
    holds the previous production build; the next incremental run re-merges the
    overlap, which is safe because the merge is idempotent on service_request_id.

    TRANSFORMER needs OWNERSHIP of both schemas and CREATE SCHEMA on the
    database. Grants follow the swapped objects, so REPORTER must be granted on
    both GOLD and GOLD_AUDIT (ADR 009).
#}

    {% set prod_schema  = target.schema %}
    {% set audit_schema = target.schema ~ audit_suffix %}
    {% set db           = target.database %}

    {# SWAP needs both schemas to exist; GOLD may not on the first publish. #}
    {% do run_query('create schema if not exists ' ~ db ~ '.' ~ prod_schema) %}

    {% do run_query(
        'alter schema ' ~ db ~ '.' ~ audit_schema
        ~ ' swap with '  ~ db ~ '.' ~ prod_schema
    ) %}

    {{ log('Published: swapped ' ~ db ~ '.' ~ audit_schema ~ ' into ' ~ db ~ '.' ~ prod_schema, info=True) }}

{% endmacro %}
