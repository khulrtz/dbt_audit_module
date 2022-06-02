{% macro m_sentinel_audit_on_run_start() %}
      
        CREATE TABLE IF NOT EXISTS {{ target.schema }}.utl_sentinel_run_log_aux
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

{%- if execute -%} {{ log('STARTING RUN WITH INVOCATION_ID: ' ~ invocation_id , info=True) }} {%- endif -%}


{% endmacro %}