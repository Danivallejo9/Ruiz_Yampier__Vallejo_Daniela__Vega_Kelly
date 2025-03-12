-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
WITH pagos_sin_duplicados AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.order_delivered_customer_date,
        p.payment_value
    FROM olist_orders o
    JOIN olist_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
    GROUP BY 
        o.customer_id, 
        o.order_id, 
        o.order_delivered_customer_date
),
ingresos_mensuales AS (
    SELECT
        STRFTIME('%m', order_delivered_customer_date) AS month_no,
        CASE 
            WHEN STRFTIME('%m', order_delivered_customer_date) = '01' THEN 'Jan'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '02' THEN 'Feb'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '03' THEN 'Mar'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '04' THEN 'Apr'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '05' THEN 'May'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '06' THEN 'Jun'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '07' THEN 'Jul'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '08' THEN 'Aug'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '09' THEN 'Sep'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '10' THEN 'Oct'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '11' THEN 'Nov'
            WHEN STRFTIME('%m', order_delivered_customer_date) = '12' THEN 'Dec'
        END AS month,
        STRFTIME('%Y', order_delivered_customer_date) AS year,
        SUM(payment_value) AS total_ingresos
    FROM pagos_sin_duplicados
    GROUP BY month_no, month, year
)
SELECT 
    month_no,
    month,
    COALESCE(SUM(CASE WHEN year = '2016' THEN total_ingresos END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN year = '2017' THEN total_ingresos END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN year = '2018' THEN total_ingresos END), 0.00) AS Year2018
FROM ingresos_mensuales
GROUP BY month_no, month
ORDER BY month_no;