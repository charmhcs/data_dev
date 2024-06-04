{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    s3_data_naming='schema_table',
    on_schema_change='append_new_columns',
    format='parquet',
    unique_key='charge_no',
    partitioned_by=['in_date'],
    table_properties={
     'optimize_rewrite_delete_file_threshold': '2'
     },
    schema='charge',
) }}

WITH v_date AS (
    SELECT start_date,
        end_date,
        CAST(CONCAT(start_date,' 15:00:00') AS TIMESTAMP) AS start_datetime,
        CAST(CONCAT(end_date,' 14:59:59') AS TIMESTAMP) AS end_datetime
    FROM (SELECT DATE_FORMAT(DATE_ADD('day', -1, CAST(date AS TIMESTAMP)), '%Y-%m-%d')  AS start_date,
                date AS end_date
            FROM t2_common.calendar cal
            WHERE cal.date = '{{ var('current_date') }}'
        )
)
SELECT  DATE_FORMAT(date '{{ var('current_date') }}', '%Y') AS  year,
        '{{ var('current_date') }}' AS in_date,
        a.charge_no,
        st_code,
        dis_st,
        dis_ed,
        hour_st,
        hour_ed,
        sp_area,
        charge,
        CAST(b.in_date AS TIMESTAMP(6)) AS in_date_at,
        in_usr_id,
        add_charge,
        work_day,
        addr5_st,
        addr5_ed,
        area_seq,
        action_name
   FROM t1_dataALD_GRP_ST_CHARGE a
   LEFT JOIN (SELECT charge_no, MAX(action_date) in_date, action_name
                    FROM t1_dataALD_GRP_ST_CHARGE_HISTORY
                WHERE action_date_ymd BETWEEN (SELECT start_date FROM v_date) AND (SELECT end_date FROM v_date)
                    AND action_date BETWEEN (SELECT start_datetime FROM v_date) AND (SELECT end_datetime FROM v_date)
                GROUP BY charge_no, action_name) b
        ON a.charge_no = b.charge_no
  WHERE a.charge_no IN (SELECT DISTINCT charge_no
                            FROM t1_dataALD_GRP_ST_CHARGE_HISTORY
                            WHERE action_date_ymd BETWEEN (SELECT start_date FROM v_date) AND (SELECT end_date FROM v_date)
                            AND action_date BETWEEN (SELECT start_datetime FROM v_date) AND (SELECT end_datetime FROM v_date))
UNION ALL

SELECT  DATE_FORMAT(date '{{ var('current_date') }}', '%Y') AS  year,
        '{{ var('current_date') }}' AS in_date,
        charge_no,
        st_code,
        dis_st,
        dis_ed,
        hour_st,
        hour_ed,
        sp_area,
        charge,
        CAST(action_date AS TIMESTAMP(6)) AS in_date_at,
        in_usr_id,
        add_charge,
        work_day,
        addr5_st,
        addr5_ed,
        area_seq,
        action_name
   FROM t1_dataALD_GRP_ST_CHARGE_HISTORY
  WHERE action_date_ymd BETWEEN (SELECT start_date FROM v_date) AND (SELECT end_date FROM v_date)
    AND action_date BETWEEN (SELECT start_datetime FROM v_date) AND (SELECT end_datetime FROM v_date)
    AND action_name LIKE '%DELETE%'

UNION ALL

SELECT  DATE_FORMAT(date '{{ var('current_date') }}', '%Y') AS  year,
        '{{ var('current_date') }}' AS in_date,
        charge_no,
        st_code,
        dis_st,
        dis_ed,
        hour_st,
        hour_ed,
        sp_area,
        charge,
        CAST(in_date AS TIMESTAMP(6)) AS in_date_at,
        in_usr_id,
        add_charge,
        work_day,
        addr5_st,
        addr5_ed,
        area_seq,
        'NEW'
   FROM t1_dataALD_GRP_ST_CHARGE
  WHERE in_date BETWEEN (SELECT start_datetime FROM v_date) AND (SELECT end_datetime FROM v_date)
