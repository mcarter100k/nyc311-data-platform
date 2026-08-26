{{
    config(materialized = 'view')
}}

{#
  Staging passthrough over the Silver quarantine table.

  Exists so fct_service_requests can delete rejected rows without referencing a
  source directly — marts reading sources is a layer violation the architecture
  tests enforce (test_no_model_references_source_except_staging).

  Deliberately no transformation. The rows here have already been judged; this
  model only makes them addressable inside the dbt graph.
#}

select
    unique_key,
    created_date,
    closed_date,
    resolution_days,
    quarantine_reason,
    _silver_timestamp
from {{ source('silver', 'quarantine') }}
