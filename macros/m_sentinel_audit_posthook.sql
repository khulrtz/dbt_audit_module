{% macro m_sentinel_audit_posthook() %}
    {%- set dbt_type = config.get('materialized') | upper -%}
    {%- if dbt_type == 'VIEW' -%}
        INSERT INTO {{ target.schema }}.utl_sentinel_run_log_aux
        SELECT 
        '{{ this }}' AS DBT_STEP,
        '{{dbt_type}}' AS DBT_MAT_TYPE,
        'CREATE OR REPLACE VIEW AS SELECT' AS DB_OPERATION_TYPE,
        '{{ invocation_id }}' AS INVOCATION_ID,
        NULL AS TS_STARTED,
        CURRENT_TIMESTAMP AS TS_FINISHED,
        'SUCCEEDED' AS STATUS,
        NULL AS DELTA_TABLE_VERSION,
        NULL AS N_TOTAL_ROWS,
        NULL AS N_COPY_ROWS,
        NULL AS N_INSERT_ROWS,
        NULL AS N_UPDATE_ROWS
    {%- else -%}
        WITH 
        A AS (DESCRIBE HISTORY {{this}}),
        A2 AS (SELECT * FROM A WHERE VERSION = (SELECT MAX(VERSION) FROM A) ),
        A3 AS (
        SELECT 
        VERSION AS DELTA_TABLE_VERSION,
        operationMetrics.numOutputRows AS N_TOTAL_ROWS,
        operationMetrics.numTargetRowsCopied AS N_COPY_ROWS,
        operationMetrics.numTargetRowsInserted AS N_INSERT_ROWS,
        operationMetrics.numTargetRowsUpdated AS N_UPDATE_ROWS,
        OPERATION
        FROM A2 WHERE operation = 'MERGE'
        UNION ALL 
        SELECT 
        VERSION AS DELTA_TABLE_VERSION,
        operationMetrics.numOutputRows AS N_TOTAL_ROWS,
        NULL AS N_COPY_ROWS,
        NULL AS N_INSERT_ROWS,
        NULL AS N_UPDATE_ROWS,
        OPERATION
        FROM A2 WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT','WRITE')
        UNION ALL
        SELECT 
        VERSION AS DELTA_TABLE_VERSION,
        NULL AS N_TOTAL_ROWS,
        NULL AS N_COPY_ROWS,
        NULL AS N_INSERT_ROWS,
        NULL AS N_UPDATE_ROWS,
        OPERATION
        FROM A2 WHERE operation IN ('CREATE TABLE'))
        INSERT INTO {{ target.schema }}.utl_sentinel_run_log_aux
        SELECT
        '{{ this }}' AS DBT_STEP,
        '{{dbt_type}}' AS DBT_MAT_TYPE,
        OPERATION AS DB_OPERATION_TYPE,
        '{{ invocation_id }}' AS INVOCATION_ID,
        NULL AS TS_STARTED,
        CURRENT_TIMESTAMP AS TS_FINISHED,
        'SUCCEEDED' AS STATUS,
        DELTA_TABLE_VERSION AS DELTA_TABLE_VERSION,
        N_TOTAL_ROWS AS N_TOTAL_ROWS,
        N_COPY_ROWS AS N_COPY_ROWS,
        N_INSERT_ROWS AS N_INSERT_ROWS,
        N_UPDATE_ROWS AS N_UPDATE_ROWS
        FROM A3
    {%- endif -%}
{% endmacro %}