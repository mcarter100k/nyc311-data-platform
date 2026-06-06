# modules/snowflake-foundation/main.tf
#
# Provisions the complete Snowflake object hierarchy for the NYC 311 platform.
# Follows least-privilege: each role receives the minimum grants required for its
# function. No role can read or write outside its designated layer.
#
# Role access matrix:
#   LOADER      → BRONZE write (INSERT only — append-only enforced at privilege layer)
#   TRANSFORMER → BRONZE read, SILVER write, GOLD write
#   REPORTER    → GOLD read-only
#   ADMIN       → full control over all platform objects

terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89.0"
    }
  }
}

locals {
  # Append environment suffix to all object names for environment isolation.
  # PROD uses bare names to keep monitoring dashboards clean.
  name_suffix = var.environment == "prod" ? "" : "_${upper(var.environment)}"

  db_name = "${var.database_name}${local.name_suffix}"

  # Fully-qualified schema identifiers used in grant blocks.
  fq_bronze = "\"${local.db_name}\".\"BRONZE\""
  fq_silver = "\"${local.db_name}\".\"SILVER\""
  fq_gold   = "\"${local.db_name}\".\"GOLD\""

  # Role names exported via outputs.
  role_admin       = "NYC311_ADMIN${local.name_suffix}"
  role_loader      = "NYC311_LOADER${local.name_suffix}"
  role_transformer = "NYC311_TRANSFORMER${local.name_suffix}"
  role_reporter    = "NYC311_REPORTER${local.name_suffix}"
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

resource "snowflake_database" "nyc311" {
  name    = local.db_name
  comment = "NYC 311 Data Platform — ${var.environment} environment"

  # Production retains 7 days of time-travel; lower environments use 1 to
  # reduce storage costs.
  data_retention_time_in_days = var.environment == "prod" ? 7 : 1
}

# ---------------------------------------------------------------------------
# Schemas — Bronze / Silver / Gold (medallion architecture)
# ---------------------------------------------------------------------------

resource "snowflake_schema" "bronze" {
  database = snowflake_database.nyc311.name
  name     = "BRONZE"
  comment  = "Raw ingestion zone — append-only, schema-on-read"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.nyc311.name
  name     = "SILVER"
  comment  = "Cleaned and typed layer — deduplicated, typed, validated"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.nyc311.name
  name     = "GOLD"
  comment  = "Dimensional model — dbt staging, intermediate, and mart models"
}

# ---------------------------------------------------------------------------
# Virtual Warehouse
# ---------------------------------------------------------------------------

resource "snowflake_warehouse" "nyc311" {
  name    = "NYC311_WH${local.name_suffix}"
  comment = "Shared compute for dbt transformations and BI reporting"

  warehouse_size = var.warehouse_size

  # Auto-suspend after inactivity and resume on first query.
  # Initially suspended prevents idle credit consumption on deployment.
  auto_suspend        = var.auto_suspend_seconds
  auto_resume         = true
  initially_suspended = true

  # STANDARD is appropriate for mixed batch + interactive workloads at this scale.
  warehouse_type = "STANDARD"
}

# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------

resource "snowflake_role" "admin" {
  name    = local.role_admin
  comment = "Full administrative control over all NYC 311 platform objects"
}

resource "snowflake_role" "loader" {
  name    = local.role_loader
  comment = "Writes raw data to BRONZE schema; no access to SILVER or GOLD"
}

resource "snowflake_role" "transformer" {
  name    = local.role_transformer
  comment = "Reads BRONZE, writes SILVER and GOLD (dbt service role)"
}

resource "snowflake_role" "reporter" {
  name    = local.role_reporter
  comment = "Read-only access to GOLD schema for BI tooling"
}

# ---------------------------------------------------------------------------
# Role hierarchy — grant all custom roles up to SYSADMIN so that SYSADMIN
# can always manage objects regardless of which role created them.
# ---------------------------------------------------------------------------

resource "snowflake_grant_account_role" "admin_to_sysadmin" {
  role_name        = snowflake_role.admin.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "loader_to_sysadmin" {
  role_name        = snowflake_role.loader.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transformer_to_sysadmin" {
  role_name        = snowflake_role.transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "reporter_to_sysadmin" {
  role_name        = snowflake_role.reporter.name
  parent_role_name = "SYSADMIN"
}

# ---------------------------------------------------------------------------
# ADMIN grants — full control over all platform objects
# ---------------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "admin_db" {
  account_role_name = snowflake_role.admin.name
  privileges        = ["USAGE", "MODIFY", "MONITOR", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "admin_all_schemas_in_db" {
  account_role_name = snowflake_role.admin.name
  privileges        = ["USAGE", "MODIFY", "MONITOR", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    all_schemas_in_database = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "admin_future_schemas_in_db" {
  account_role_name = snowflake_role.admin.name
  privileges        = ["USAGE", "MODIFY", "MONITOR", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    future_schemas_in_database = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "admin_warehouse" {
  account_role_name = snowflake_role.admin.name
  privileges        = ["USAGE", "OPERATE", "MONITOR"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.nyc311.name
  }
}

# ---------------------------------------------------------------------------
# LOADER grants — BRONZE write only
# ---------------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "loader_db_usage" {
  account_role_name = snowflake_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_warehouse_usage" {
  account_role_name = snowflake_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_bronze_schema" {
  account_role_name = snowflake_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    schema_name = local.fq_bronze
  }
  depends_on = [snowflake_schema.bronze]
}

# LOADER can INSERT and SELECT on future BRONZE tables. SELECT enables load
# verification queries immediately after write. Bronze is append-only — UPDATE
# and TRUNCATE are intentionally omitted to enforce that contract at the
# privilege layer, not just by convention.
resource "snowflake_grant_privileges_to_account_role" "loader_bronze_future_tables" {
  account_role_name = snowflake_role.loader.name
  privileges        = ["SELECT", "INSERT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.fq_bronze
    }
  }
  depends_on = [snowflake_schema.bronze]
}

resource "snowflake_grant_privileges_to_account_role" "loader_bronze_future_stages" {
  account_role_name = snowflake_role.loader.name
  # READ and WRITE cover both internal named stages (PUT/GET/LIST/REMOVE) and
  # are accepted by Snowflake for external stages as well. USAGE is valid only
  # for external stages and is not recognized for internal stages — omit it to
  # avoid a grant error when internal stages are present in BRONZE.
  privileges = ["READ", "WRITE"]
  on_schema_object {
    future {
      object_type_plural = "STAGES"
      in_schema          = local.fq_bronze
    }
  }
  depends_on = [snowflake_schema.bronze]
}

# ---------------------------------------------------------------------------
# TRANSFORMER grants — read BRONZE, write SILVER, write GOLD
# ---------------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "transformer_db_usage" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse_usage" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.nyc311.name
  }
}

# BRONZE — read access only; TRANSFORMER must never write to the raw layer.
resource "snowflake_grant_privileges_to_account_role" "transformer_bronze_schema_usage" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.fq_bronze
  }
  depends_on = [snowflake_schema.bronze]
}

resource "snowflake_grant_privileges_to_account_role" "transformer_bronze_future_tables" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.fq_bronze
    }
  }
  depends_on = [snowflake_schema.bronze]
}

# SILVER — full write; dbt creates and populates Silver tables.
resource "snowflake_grant_privileges_to_account_role" "transformer_silver_schema" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    schema_name = local.fq_silver
  }
  depends_on = [snowflake_schema.silver]
}

resource "snowflake_grant_privileges_to_account_role" "transformer_silver_future_tables" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.fq_silver
    }
  }
  depends_on = [snowflake_schema.silver]
}

resource "snowflake_grant_privileges_to_account_role" "transformer_silver_future_views" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = local.fq_silver
    }
  }
  depends_on = [snowflake_schema.silver]
}

# GOLD — full write; dbt marts and dim/fact tables live here.
resource "snowflake_grant_privileges_to_account_role" "transformer_gold_schema" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = local.fq_gold
  }
  depends_on = [snowflake_schema.gold]
}

resource "snowflake_grant_privileges_to_account_role" "transformer_gold_future_tables" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.fq_gold
    }
  }
  depends_on = [snowflake_schema.gold]
}

resource "snowflake_grant_privileges_to_account_role" "transformer_gold_future_views" {
  account_role_name = snowflake_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = local.fq_gold
    }
  }
  depends_on = [snowflake_schema.gold]
}

# ---------------------------------------------------------------------------
# REPORTER grants — GOLD read-only
# ---------------------------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "reporter_db_usage" {
  account_role_name = snowflake_role.reporter.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_warehouse_usage" {
  account_role_name = snowflake_role.reporter.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.nyc311.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_gold_schema_usage" {
  account_role_name = snowflake_role.reporter.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.fq_gold
  }
  depends_on = [snowflake_schema.gold]
}

resource "snowflake_grant_privileges_to_account_role" "reporter_gold_future_tables" {
  account_role_name = snowflake_role.reporter.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.fq_gold
    }
  }
  depends_on = [snowflake_schema.gold]
}

resource "snowflake_grant_privileges_to_account_role" "reporter_gold_future_views" {
  account_role_name = snowflake_role.reporter.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = local.fq_gold
    }
  }
  depends_on = [snowflake_schema.gold]
}
