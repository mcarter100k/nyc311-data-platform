{% macro publish_gold(audit_suffix='_audit') %}
{#
    Publish step of the write-audit-publish pattern.

    NOT INVOKED BY ANYTHING IN THIS REPOSITORY — it is a Snowflake-target
    operation with no caller. This comment used to open "The Airflow dbt_build
    task runs: dbt build --vars ...", which was false: that task runs
    `local/local_runner.py --only 4` against DuckDB, which has no ALTER SCHEMA
    SWAP and no audit_suffix branch in its copy of generate_schema_name. Run
    the two steps below by hand against a Snowflake target.

    Step one builds AND tests every model in GOLD_AUDIT:

        dbt build --vars '{"audit_suffix": "_audit"}'

    (see generate_schema_name). Nothing is published yet — if any model or test
    fails, the command fails and GOLD is untouched, so BI consumers never see
    unvalidated data.

    Step two publishes atomically:

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

    Grants: Snowflake grants attach to schema OBJECTS, and this swap renames
    objects — so REPORTER's grants must be defined symmetrically on both GOLD
    and GOLD_AUDIT or BI access breaks on the first publish. See ADR 009.
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
