{% macro publish_gold(audit_suffix='_audit') %}
{#
    Publish step of the write-audit-publish pattern.

    The Airflow dbt_build task runs:

        dbt build --vars '{"audit_suffix": "_audit"}'

    which builds AND tests every model in GOLD_AUDIT (see generate_schema_name).
    Nothing is published yet — if any model or test fails, the task fails and
    GOLD is untouched, so BI consumers never see unvalidated data.

    This operation then publishes atomically:

        dbt run-operation publish_gold

    swapping the audited schema into place with ALTER SCHEMA ... SWAP WITH,
    a single atomic metadata operation in Snowflake.

    Incremental note: after a swap, GOLD_AUDIT holds the previous production
    build (the schemas ping-pong, so the audit schema is always exactly one run
    behind). The next incremental run merges everything Silver touched since
    that state — the merge is idempotent on service_request_id, so re-processing
    the overlap is safe.

    Permissions: the TRANSFORMER role needs OWNERSHIP of both schemas (it
    creates them) and CREATE SCHEMA on the database for the first run.
#}

    {% set prod_schema  = target.schema %}
    {% set audit_schema = target.schema ~ audit_suffix %}
    {% set db           = target.database %}

    {# First run: GOLD may not exist yet if everything so far was built via the
       audit schema. SWAP requires both sides to exist. #}
    {% do run_query('create schema if not exists ' ~ db ~ '.' ~ prod_schema) %}

    {% do run_query(
        'alter schema ' ~ db ~ '.' ~ audit_schema
        ~ ' swap with '  ~ db ~ '.' ~ prod_schema
    ) %}

    {{ log('Published: swapped ' ~ db ~ '.' ~ audit_schema ~ ' into ' ~ db ~ '.' ~ prod_schema, info=True) }}

{% endmacro %}
