{#
  Versioned CDC (change data capture) incremental strategy.

  Models using `incremental_strategy='cdc'` output, in addition to their business
  columns:
    - the business key column(s) named by `unique_key`
    - an integer version column (config `cdc_version_column`, default `_cdc_version`)
    - an operation column (config `cdc_operation_column`, default `_cdc_op`) whose
      values are mapped to upsert/delete via `cdc_upsert_values` (default
      ['upsert']) and `cdc_delete_values` (default ['delete']), matched
      case-insensitively

  For every business key only the change with the highest version is applied:
  older versions are ignored, identical replays of the applied change are no-ops,
  and conflicting contents at the same version raise an error. The highest
  applied version per key is tracked in a persistent `<model>__dbt_cdc_state`
  table that is updated in the same transaction as the business table, so deletes
  are remembered across runs and a key can be resurrected by a newer version.
  The version/operation columns never land in the business table.
#}

{% macro get_incremental_cdc_sql(args_dict) %}
  {{ return(adapter.dispatch('get_incremental_cdc_sql', 'dbt')(args_dict)) }}
{% endmacro %}


{% macro duckdb__get_incremental_cdc_sql(args_dict) %}
  {%- set target_relation = args_dict['target_relation'] -%}
  {%- set temp_relation = args_dict['temp_relation'] -%}
  {%- set unique_key = args_dict['unique_key'] -%}
  {%- set dest_columns = args_dict['dest_columns'] -%}

  {%- set cfg = duckdb__cdc_config() -%}
  {%- set key_cols = duckdb__cdc_key_cols(unique_key) -%}
  {%- set state_relation = duckdb__cdc_state_relation(target_relation) -%}
  {%- set business_cols = dest_columns | map(attribute='name') | list -%}

  {{ return(duckdb__cdc_apply_script(target_relation, state_relation, temp_relation, key_cols, business_cols, cfg)) }}
{% endmacro %}


{% macro duckdb__cdc_config() %}
  {#- Read and validate the cdc_* model configs. Raises on any problem. -#}
  {%- set errors = [] -%}
  {%- set version_column = config.get('cdc_version_column', '_cdc_version') -%}
  {%- set operation_column = config.get('cdc_operation_column', '_cdc_op') -%}
  {%- set upsert_values = config.get('cdc_upsert_values', ['upsert']) -%}
  {%- set delete_values = config.get('cdc_delete_values', ['delete']) -%}

  {%- do validate_string_field(version_column, 'cdc_version_column', errors) -%}
  {%- do validate_string_field(operation_column, 'cdc_operation_column', errors) -%}
  {%- do validate_string_list_field(upsert_values, 'cdc_upsert_values', errors) -%}
  {%- do validate_string_list_field(delete_values, 'cdc_delete_values', errors) -%}

  {%- if not errors -%}
    {%- if version_column | lower == operation_column | lower -%}
      {%- do errors.append("cdc_version_column and cdc_operation_column must be different columns, both are set to: " ~ version_column) -%}
    {%- endif -%}
    {%- if upsert_values | length == 0 -%}
      {%- do errors.append("cdc_upsert_values must contain at least one value") -%}
    {%- endif -%}
    {%- if delete_values | length == 0 -%}
      {%- do errors.append("cdc_delete_values must contain at least one value") -%}
    {%- endif -%}
  {%- endif -%}

  {%- if not errors -%}
    {%- set upsert_norm = upsert_values | map('upper') | list -%}
    {%- set delete_norm = delete_values | map('upper') | list -%}
    {%- set overlap = upsert_norm | select('in', delete_norm) | list -%}
    {%- if overlap | length > 0 -%}
      {%- do errors.append("cdc_upsert_values and cdc_delete_values must not overlap (operation values are matched case-insensitively), found in both: " ~ overlap | join(', ')) -%}
    {%- endif -%}
  {%- endif -%}

  {%- if errors -%}
    {{ exceptions.raise_compiler_error("CDC configuration errors:\n" ~ errors | join('\n')) }}
  {%- endif -%}

  {{ return({
    'version_column': version_column,
    'operation_column': operation_column,
    'upsert_values': upsert_norm,
    'delete_values': delete_norm,
  }) }}
{% endmacro %}


{% macro duckdb__cdc_key_cols(unique_key) %}
  {#- Normalize unique_key to a list of plain column names. Raises if missing. -#}
  {%- if not unique_key -%}
    {{ exceptions.raise_compiler_error(
      "The 'cdc' incremental strategy requires a unique_key config naming the business "
      ~ "key column(s), so that changes can be matched by key and version.") }}
  {%- endif -%}
  {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string -%}
    {%- set keys = unique_key | list -%}
  {%- else -%}
    {%- set keys = [unique_key] -%}
  {%- endif -%}
  {%- for key in keys -%}
    {%- if key is not string -%}
      {{ exceptions.raise_compiler_error(
        "The 'cdc' incremental strategy requires unique_key to name plain column(s), found: " ~ key) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(keys) }}
{% endmacro %}


{% macro duckdb__cdc_state_relation(target_relation) %}
  {#- The persistent per-model table holding the highest applied version, the
      applied operation and a content hash for every business key. -#}
  {{ return(target_relation.incorporate(
      path={'identifier': target_relation.identifier ~ '__dbt_cdc_state'},
      type='table')) }}
{% endmacro %}


{% macro duckdb__cdc_validate_batch_columns(batch_columns, key_cols, cfg) %}
  {#- Compile-time validation of the model output against the cdc configuration. -#}
  {%- set available = batch_columns | map(attribute='name') | map('lower') | list -%}
  {%- set errors = [] -%}
  {%- for key in key_cols -%}
    {%- if key | lower not in available -%}
      {%- do errors.append("unique_key column '" ~ key ~ "' is not present in the model output") -%}
    {%- endif -%}
    {%- if key | lower in [cfg['version_column'] | lower, cfg['operation_column'] | lower] -%}
      {%- do errors.append("unique_key column '" ~ key ~ "' must not be the version or operation column") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if cfg['version_column'] | lower not in available -%}
    {%- do errors.append("version column '" ~ cfg['version_column'] ~ "' (cdc_version_column) is not present in the model output") -%}
  {%- endif -%}
  {%- if cfg['operation_column'] | lower not in available -%}
    {%- do errors.append("operation column '" ~ cfg['operation_column'] ~ "' (cdc_operation_column) is not present in the model output") -%}
  {%- endif -%}
  {%- if errors -%}
    {{ exceptions.raise_compiler_error(
      "CDC model output is missing required column(s):\n" ~ errors | join('\n')
      ~ "\nAvailable columns: " ~ (batch_columns | map(attribute='name') | join(', '))) }}
  {%- endif -%}

  {#- the version column must have an integer type so versions compare and store exactly -#}
  {%- set integer_types = ['tinyint', 'smallint', 'integer', 'bigint', 'hugeint',
                           'utinyint', 'usmallint', 'uinteger', 'ubigint', 'uhugeint'] -%}
  {%- for col in batch_columns -%}
    {%- if col.name | lower == cfg['version_column'] | lower
           and col.data_type | lower not in integer_types -%}
      {{ exceptions.raise_compiler_error(
        "CDC version column '" ~ col.name ~ "' must have an integer type, found "
        ~ col.data_type ~ ". Cast it to a bigint in the model SQL.") }}
    {%- endif -%}
  {%- endfor -%}
{% endmacro %}


{% macro duckdb__cdc_business_column_names(batch_columns, cfg) %}
  {#- Model output columns minus the version/operation columns. -#}
  {%- set meta_cols = [cfg['version_column'] | lower, cfg['operation_column'] | lower] -%}
  {%- set names = [] -%}
  {%- for col in batch_columns -%}
    {%- if col.name | lower not in meta_cols -%}
      {%- do names.append(col.name) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(names) }}
{% endmacro %}


{% macro duckdb__cdc_business_view(temp_relation, key_cols, cfg) %}
  {#- A temp view exposing only the business columns of the CDC batch, so that
      on_schema_change processing and column type expansion never see the
      version/operation columns. -#}
  {%- set batch_columns = adapter.get_columns_in_relation(temp_relation) -%}
  {%- do duckdb__cdc_validate_batch_columns(batch_columns, key_cols, cfg) -%}
  {%- set business_cols = duckdb__cdc_business_column_names(batch_columns, cfg) -%}
  {%- set view_relation = temp_relation.incorporate(
      path={'identifier': temp_relation.identifier ~ '__cdc_business'}, type='view') -%}
  {% do run_query('create or replace temp view ' ~ view_relation.render() ~ ' as select '
      ~ get_quoted_csv(business_cols) ~ ' from ' ~ temp_relation.render()) %}
  {{ return(view_relation) }}
{% endmacro %}


{% macro duckdb__cdc_full_load_sql(target_relation, batch_relation, unique_key, partitioned_by=none, sorted_by=none) %}
  {#- First load and full-refresh: (re)create the business table empty (without
      the version/operation columns) and load the batch through the standard CDC
      apply rules; the state table is (re)created empty by the apply script. -#}
  {%- set cfg = duckdb__cdc_config() -%}
  {%- set key_cols = duckdb__cdc_key_cols(unique_key) -%}
  {%- set batch_columns = adapter.get_columns_in_relation(batch_relation) -%}
  {%- do duckdb__cdc_validate_batch_columns(batch_columns, key_cols, cfg) -%}
  {%- set business_cols = duckdb__cdc_business_column_names(batch_columns, cfg) -%}
  {%- set state_relation = duckdb__cdc_state_relation(target_relation) -%}
  {{ create_table_as(False, target_relation, 'select ' ~ get_quoted_csv(business_cols) ~ ' from ' ~ batch_relation.render() ~ ' where 1=0', 'sql', partitioned_by=partitioned_by, sorted_by=sorted_by) }}
  {{ duckdb__cdc_apply_script(target_relation, state_relation, batch_relation, key_cols, business_cols, cfg) }}
{% endmacro %}


{% macro duckdb__cdc_string_literals(values) %}
  {%- set literals = [] -%}
  {%- for value in values -%}
    {%- do literals.append("'" ~ (value | replace("'", "''")) ~ "'") -%}
  {%- endfor -%}
  {{ return(literals | join(', ')) }}
{% endmacro %}


{% macro duckdb__cdc_key_join(left_alias, right_alias, key_quoted) -%}
  {%- for key in key_quoted -%}
    {{ left_alias }}.{{ key }} = {{ right_alias }}.{{ key }}{{ ' and ' if not loop.last }}
  {%- endfor -%}
{%- endmacro %}


{% macro duckdb__cdc_key_concat(key_quoted, alias=none) -%}
  concat_ws(',', {% for key in key_quoted %}cast({{ (alias ~ '.') if alias else '' }}{{ key }} as varchar){{ ', ' if not loop.last }}{% endfor %})
{%- endmacro %}


{% macro duckdb__cdc_apply_script(target_relation, state_relation, batch_relation, key_cols, business_cols, cfg) %}
  {#- The versioned CDC apply, executed as a single multi-statement script inside
      the model transaction: validate the batch, reduce it to the highest version
      per key, reject conflicting contents, then apply upserts/deletes to the
      business table and record the applied versions in the state table.
      Internal columns are named __dbt_cdc_* so they cannot collide with business
      columns of the batch. -#}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set key_quoted = [] -%}
  {%- for key in key_cols -%}
    {%- do key_quoted.append(adapter.quote(key)) -%}
  {%- endfor -%}
  {%- set key_csv = key_quoted | join(', ') -%}
  {%- set business_csv = get_quoted_csv(business_cols) -%}
  {%- set version_col = adapter.quote(cfg['version_column']) -%}
  {%- set op_col = adapter.quote(cfg['operation_column']) -%}

  {%- set delete_literals = duckdb__cdc_string_literals(cfg['delete_values']) -%}
  {%- set upsert_literals = duckdb__cdc_string_literals(cfg['upsert_values']) -%}
  {%- set valid_ops = (cfg['delete_values'] + cfg['upsert_values']) | join(', ') -%}

  {%- set hash_inputs = [] -%}
  {%- for col in business_cols -%}
    {%- do hash_inputs.append('coalesce(cast(' ~ adapter.quote(col) ~ ' as varchar), chr(30))') -%}
  {%- endfor -%}

  {{ sql_header if sql_header is not none }}

  -- ensure the version state table exists (created empty on first use; dropped
  -- upfront by full-refresh so it is rebuilt from the new batch)
  create table if not exists {{ state_relation }} as
  select {{ key_csv }},
         cast(null as bigint) as _dbt_cdc_version,
         cast(null as varchar) as _dbt_cdc_op,
         cast(null as varchar) as _dbt_cdc_hash
  from {{ batch_relation }}
  where 1=0;

  -- normalize the batch: cast the version, map the operation, hash the content
  create or replace temp table dbt_cdc_norm as
  with parsed as (
    select {{ business_csv }},
           cast({{ version_col }} as bigint) as __dbt_cdc_ver,
           case
             when upper(cast({{ op_col }} as varchar)) in ({{ delete_literals }}) then 'delete'
             when upper(cast({{ op_col }} as varchar)) in ({{ upsert_literals }}) then 'upsert'
           end as __dbt_cdc_op,
           cast({{ op_col }} as varchar) as __dbt_cdc_op_raw
    from {{ batch_relation }}
  )
  select *,
         md5(concat_ws(chr(29), coalesce(__dbt_cdc_op, chr(30)), {{ hash_inputs | join(', ') }})) as __dbt_cdc_hash
  from parsed;

  -- every change must carry its business key
  select error('cdc strategy: null value in unique_key column(s) ({{ key_cols | join(", ") }}) in model {{ model['name'] }}')
  from dbt_cdc_norm
  where {{ key_quoted | join(' is null or ') }} is null;

  -- every change must carry a version
  select error('cdc strategy: null version for key ' || {{ duckdb__cdc_key_concat(key_quoted) }} || ' in model {{ model['name'] }}')
  from dbt_cdc_norm
  where __dbt_cdc_ver is null;

  -- every operation must map to upsert or delete
  select error('cdc strategy: invalid operation ''' || coalesce(__dbt_cdc_op_raw, '<null>') || ''' for key ' || {{ duckdb__cdc_key_concat(key_quoted) }} || ' in model {{ model['name'] }}; expected one of {{ valid_ops }} (case-insensitive)')
  from dbt_cdc_norm
  where __dbt_cdc_op is null;

  -- within the batch, rows for one key at its highest version must be identical
  select error('cdc strategy: conflicting contents at version ' || cast(__dbt_cdc_ver as varchar) || ' for key ' || {{ duckdb__cdc_key_concat(key_quoted) }} || ' in model {{ model['name'] }}')
  from (
    select {{ key_csv }}, __dbt_cdc_ver, __dbt_cdc_hash
    from dbt_cdc_norm
    qualify rank() over (partition by {{ key_csv }} order by __dbt_cdc_ver desc) = 1
  ) atmax
  group by {{ key_csv }}, __dbt_cdc_ver
  having count(distinct __dbt_cdc_hash) > 1;

  -- one change per key: the highest version in the batch
  create or replace temp table dbt_cdc_reduced as
  select * exclude (__dbt_cdc_op_raw)
  from dbt_cdc_norm
  qualify row_number() over (partition by {{ key_csv }} order by __dbt_cdc_ver desc, __dbt_cdc_hash) = 1;

  -- a replayed version must carry the same contents as what was already applied
  select error('cdc strategy: change for key ' || {{ duckdb__cdc_key_concat(key_quoted, 'r') }} || ' at version ' || cast(r.__dbt_cdc_ver as varchar) || ' conflicts with a different change already applied at that version in model {{ model['name'] }}')
  from dbt_cdc_reduced r
  join {{ state_relation }} s on {{ duckdb__cdc_key_join('r', 's', key_quoted) }}
  where r.__dbt_cdc_ver = s._dbt_cdc_version and r.__dbt_cdc_hash <> s._dbt_cdc_hash;

  -- keep only changes newer than what the state table has already applied
  create or replace temp table dbt_cdc_apply as
  select r.*
  from dbt_cdc_reduced r
  left join {{ state_relation }} s on {{ duckdb__cdc_key_join('r', 's', key_quoted) }}
  where s.{{ key_quoted[0] }} is null or r.__dbt_cdc_ver > s._dbt_cdc_version;

  -- apply to the business table: replace upserted keys, remove deleted keys
  delete from {{ target_relation }} as t
  using dbt_cdc_apply as a
  where {{ duckdb__cdc_key_join('t', 'a', key_quoted) }};

  insert into {{ target_relation }} ({{ business_csv }})
  select {{ business_csv }} from dbt_cdc_apply
  where __dbt_cdc_op = 'upsert';

  -- record the applied versions (deletes included, as tombstones)
  delete from {{ state_relation }} as s
  using dbt_cdc_apply as a
  where {{ duckdb__cdc_key_join('s', 'a', key_quoted) }};

  insert into {{ state_relation }} ({{ key_csv }}, _dbt_cdc_version, _dbt_cdc_op, _dbt_cdc_hash)
  select {{ key_csv }}, __dbt_cdc_ver, __dbt_cdc_op, __dbt_cdc_hash from dbt_cdc_apply;

  drop table dbt_cdc_norm;
  drop table dbt_cdc_reduced;
  drop table dbt_cdc_apply;
{% endmacro %}
