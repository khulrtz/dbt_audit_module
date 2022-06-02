{% macro m_sentinel_audit_prehook() %}


    {%- set dbt_type = config.get('materialized') | upper -%}


    INSERT INTO {{ target.schema }}.utl_sentinel_run_log_aux
    SELECT 
    '{{ this }}' AS DBT_STEP,
    '{{dbt_type}}' AS DBT_MAT_TYPE,
    NULL AS DB_OPERATION_TYPE,
    '{{ invocation_id }}' AS INVOCATION_ID,
    CURRENT_TIMESTAMP AS TS_STARTED,
    NULL AS TS_FINISHED,
    'RUNNING' AS STATUS,
    NULL AS DELTA_TABLE_VERSION,
    NULL AS N_TOTAL_ROWS,
    NULL AS N_COPY_ROWS,
    NULL AS N_INSERT_ROWS,
    NULL AS N_UPDATE_ROWS

{% endmacro %}