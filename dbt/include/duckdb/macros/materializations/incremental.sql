{% materialization incremental, adapter="duckdb", supported_languages=['sql', 'python'] -%}

  {%- set language = model['language'] -%}
  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set incremental_strategy = config.get('incremental_strategy') or 'default' -%}
  {%- set cdc_mode = incremental_strategy == 'cdc' -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set partitioned_by = none -%}
  {%- set sorted_by = none -%}
  {%- if existing_relation is none or full_refresh_mode -%}
    {%- set partitioned_by = duckdb__get_partitioned_by(target_relation, false) -%}
    {%- set sorted_by = duckdb__get_sorted_by(target_relation, false) -%}
  {%- endif -%}
  {%- set skip_auto_begin = (partitioned_by or sorted_by) and adapter.is_ducklake(target_relation) -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% set to_drop = [] %}
  {% set dropped_dependents = [] %}
   -- if not using a temporary table we will update the temp relation to use a different temp schema ("dbt_temp" by default)
   -- for microbatch with concurrent batches, include batch timestamps in the identifier to avoid collisions
  {%- set batch_id = adapter.batch_id_for_model(model) -%}
  {% set temporary_relation = duckdb__dispatch_temporary_relation(target_relation, batch_id) %}
  {% set temp_relation = temporary_relation.relation %}
  {% set temporary = temporary_relation.temporary %}
  {% if not temporary %}
    {% do to_drop.append(temp_relation) %}
  {% endif %}
  {% set python_staging_relation = none %}

  {% if cdc_mode %}
    -- the cdc strategy loads versioned change batches through its own apply rules
    {% if language != 'sql' %}
      {{ exceptions.raise_compiler_error("The 'cdc' incremental strategy only supports SQL models, got language: " ~ language) }}
    {% endif %}
    -- validates the cdc_* configs and unique_key, raising on any problem
    {%- set cdc_cfg = duckdb__cdc_config() -%}
    {%- set cdc_key_cols = duckdb__cdc_key_cols(unique_key) -%}
    {%- set cdc_state_relation = duckdb__cdc_state_relation(target_relation) -%}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% if cdc_mode %}
      {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language)) %}
      {% set build_sql = duckdb__cdc_full_load_sql(target_relation, temp_relation, unique_key, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% endif %}
  {% elif full_refresh_mode and has_foreign_key_constraints(existing_relation) %}
    {#-- Tables with foreign keys must not be renamed (see has_foreign_key_constraints) --#}
    {% set dropped_dependents = drop_relation_for_rebuild(existing_relation) %}
    {% if cdc_mode %}
      {#-- full-refresh rebuilds the version state from the new batch, in the same transaction --#}
      {% do adapter.drop_relation(cdc_state_relation) %}
      {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language)) %}
      {% set build_sql = duckdb__cdc_full_load_sql(target_relation, temp_relation, unique_key, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% endif %}
  {% elif full_refresh_mode %}
    {% if cdc_mode %}
      {#-- drop and rebuild in-transaction so a failed batch rolls back both tables --#}
      {% set dropped_dependents = drop_relation_for_rebuild(existing_relation) %}
      {% do adapter.drop_relation(cdc_state_relation) %}
      {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language)) %}
      {% set build_sql = duckdb__cdc_full_load_sql(target_relation, temp_relation, unique_key, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
    {% else %}
      {% set build_sql = create_table_as(False, intermediate_relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
      {% set need_swap = true %}
    {% endif %}
  {% else %}
    {% if language == 'python' %}
      {% set build_python = create_table_as(temporary, temp_relation, compiled_code, language, partitioned_by=none, sorted_by=none) %}
      {% call statement("pre", language=language) %}
        {{- build_python }}
      {% endcall %}
      {# py_write_table creates a regular table for the staging relation even when it is meant to be temporary #}
      {% if temporary %}
        {% set python_staging_relation = temp_relation %}
      {% endif %}
    {% else %} {# SQL #}
      {% do run_query(create_table_as(temporary, temp_relation, compiled_code, language, partitioned_by=none, sorted_by=none)) %}
    {% endif %}
    {% if cdc_mode %}
      {#-- schema changes only see the business columns; the version/operation
          columns of the batch must never be added to the business table --#}
      {%- set cdc_schema_relation = duckdb__cdc_business_view(temp_relation, cdc_key_cols, cdc_cfg) -%}
      {% do adapter.expand_target_column_types(
               from_relation=cdc_schema_relation,
               to_relation=target_relation) %}
      {% set dest_columns = process_schema_changes(on_schema_change, cdc_schema_relation, existing_relation) %}
    {% else %}
      {% do adapter.expand_target_column_types(
               from_relation=temp_relation,
               to_relation=target_relation) %}
      {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
      {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% endif %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
    {% set language = "sql" %}

  {% endif %}

  {% call statement("main", language=language, auto_begin=not skip_auto_begin) %}
      {{- build_sql }}
  {% endcall %}

  {% if need_swap %}
      {#-- Drop indexes and foreign key dependents on target relation before renaming to backup to avoid dependency errors --#}
      {% do drop_indexes_on_relation(target_relation) %}
      {% if may_be_foreign_key_target(target_relation) %}
        {% set dropped_dependents = drop_foreign_key_dependents(target_relation) %}
      {% endif %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}
  {% do restore_foreign_key_dependents(dropped_dependents) %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {# Align order with table materialization to avoid MotherDuck alter conflicts #}
  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {# On MotherDuck the temp relation is a real table; dropping it cascades indexes. Avoid extra ALTERs. #}
      {% if not adapter.is_motherduck() %}
        {% do drop_indexes_on_relation(rel) %}
      {% endif %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}
  {% if python_staging_relation is not none %}
    {#-- The relation is unqualified, so drop_relation cannot tell whether it resolves to a DuckLake
         catalog, which rejects CASCADE. The staging table has no dependents, so a plain drop is enough. --#}
    {% call statement('drop_python_staging_relation', auto_begin=False) -%}
      drop table if exists {{ python_staging_relation }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
