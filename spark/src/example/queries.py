from helpers.constant.common import CommonConstant
class Queries():

    order_record = r"""
         SELECT TO_CHAR(CAST (ordered_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY') AS year,
                TO_CHAR(CAST (ordered_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'MM') AS month,
                TO_CHAR(CAST (ordered_at AS TIMESTAMP)AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS order_date,
                CAST(order_id AS DECIMAL(38,0)) AS order_id,
                DAMO.DECRYPT_VAR_B64('{3}', order_phone_number, '') AS order_phone_number,
                CAST(order_amount AS DECIMAL(38,0)) AS order_amount,
                address,
                ordered_at,
                created_at,
                updated_at
        FROM data.order_record
        WHERE  ordered_at BETWEEN  to_date('{0}', '{2}') AND to_date('{1}', '{2}')
    """

    # worker_info
    worker_info = r"""
        SELECT  worker_id,
                worker_name,
                phone_number,
                ordered_at,
                created_at,
                updated_at
        FROM    data.worker_info
    """

    adjustment_cash = r"""
        SELECT  TO_CHAR(CAST (created_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY') AS year,
                TO_CHAR(CAST (created_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'MM') AS month,
                TO_CHAR(CAST (created_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS create_date,
                CAST(adjustment_id AS DECIMAL(38,0)) AS adjustment_id,
                adjustment_amount,
                user_id,
                created_at,
                NVL(updated_at, created_at) AS updated_at
        FROM    data.adjustment_cash
        WHERE   updated_at BETWEEN to_date('{0}', '{2}') AND to_date('{1}', '{2}')
    """
    adjustment_brand  = r"""
        SELECT  TO_CHAR(CAST (created_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY') AS year,
                TO_CHAR(CAST (created_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'MM') AS month,
                '{0}' AS load_date,
                hd_code,
                CAST(share_time AS DECIMAL(38,0)) AS share_time,
                CAST(call_rate AS DECIMAL(38,0)) AS call_rate,
                status
        FROM    data.adjustment_brand
    """

    system_base_code = r"""
        SELECT  TO_CHAR(TO_DATE('{0}','YYYY-MM-DD'), 'YYYY') AS year,
                TO_CHAR(TO_DATE('{0}','YYYY-MM-DD'), 'MM') AS month,
                '{0}' AS adjustment_date,
                adjustment_day,
                order_brand_code,
                brand_code,
                worker_id,
                worker_option
        FROM    data.system_base_code
        WHERE   adj_day = TO_CHAR(TO_DATE('{0}' ,'YYYY-MM-DD'), 'YYYYMMDD')
    """

    store_charge = r"""
        SELECT  CAST(charge_id AS DECIMAL(38,0)) AS  charge_id,
                store_id,
                charge_amount,
                addtion_charge_amount,
                area,
                work_day,
                CAST(area_sequece AS DECIMAL(38,0)) AS area_sequece
        FROM data.store_charge
    """

    store_charge_history = r"""
        SELECT  TO_CHAR(CAST (action_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY') AS year,
                TO_CHAR(CAST (action_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'MM') AS month,
                TO_CHAR(CAST (action_at AS TIMESTAMP) AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS action_date,
                CAST(charge_id AS DECIMAL(38,0)) AS  charge_id,
                store_id,
                charge_amount,
                addtion_charge_amount,
                area,
                work_day,
                CAST(area_sequece AS DECIMAL(38,0)) AS area_sequece,
                action_name,
                action_at
        FROM    data.store_charge_history
        WHERE   action_at BETWEEN to_date('{0}', '{2}') AND to_date('{1}', '{2}')
    """
