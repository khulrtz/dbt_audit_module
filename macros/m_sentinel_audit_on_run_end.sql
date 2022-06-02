{% macro m_sentinel_audit_on_run_end() %}
      {%- set create_table -%}
        CREATE TABLE IF NOT EXISTS {{ target.schema }}.utl_sentinel_run_log
        (
        DBT_STEP string,
        DBT_MAT_TYPE string,
        DB_OPERATION_TYPE string,
        INVOCATION_ID string,
        TS_STARTED timestamp,
        TS_FINISHED timestamp,
        STATUS string,
        DELTA_TABLE_VERSION bigint,
        N_TOTAL_ROWS bigint,
        N_COPY_ROWS bigint,
        N_INSERT_ROWS bigint,
        N_UPDATE_ROWS bigint
        ) USING DELTA
        PARTITIONED BY (INVOCATION_ID) 
    {%- endset -%}

    {%- set results = run_query(create_table) -%}

      INSERT INTO {{ target.schema }}.utl_sentinel_run_log
      select 
      DBT_STEP,
      DBT_MAT_TYPE,
      DB_OPERATION_TYPE,
      INVOCATION_ID,
      TS_STARTED,
      TS_FINISHED,
      'FAILED' AS STATUS,
      DELTA_TABLE_VERSION,
      N_TOTAL_ROWS,
      N_COPY_ROWS,
      N_INSERT_ROWS,
      N_UPDATE_ROWS
      from {{ target.schema }}.utl_sentinel_run_log_AUX
      where (dbt_step) in (select dbt_step from {{ target.schema }}.utl_sentinel_run_log_aux where invocation_id ='{{invocation_id}}' group by dbt_step having count(1) = 1)
      and invocation_id='{{invocation_id}}'

      UNION ALL

      select 
      DBT_STEP,
      MAX(DBT_MAT_TYPE) DBT_MAT_TYPE,
      MAX(DB_OPERATION_TYPE) DB_OPERATION_TYPE,
      INVOCATION_ID,
      MAX(TS_STARTED) TS_STARTED,
      MAX(TS_FINISHED) TS_FINISHED,
      'SUCCEEDED' AS STATUS,
      MAX(DELTA_TABLE_VERSION) DELTA_TABLE_VERSION,
      MAX(N_TOTAL_ROWS) N_TOTAL_ROWS,
      MAX(N_COPY_ROWS) N_COPY_ROWS,
      MAX(N_INSERT_ROWS) N_INSERT_ROWS,
      MAX(N_UPDATE_ROWS) N_UPDATE_ROWS
      from {{ target.schema }}.utl_sentinel_run_log_AUX
      where (dbt_step) in (select dbt_step from {{ target.schema }}.utl_sentinel_run_log_aux where invocation_id ='{{invocation_id}}' group by dbt_step having count(1) = 2)
      and invocation_id='{{invocation_id}}'
      GROUP BY DBT_STEP, INVOCATION_ID

{%- if execute -%} {{ log('FINISHING RUN WITH INVOCATION_ID: ' ~ invocation_id , info=True) }} {%- endif -%}

{% endmacro %}