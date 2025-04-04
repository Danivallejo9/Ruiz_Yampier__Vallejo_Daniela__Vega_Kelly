-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.
WITH entrega_diferencias AS (
    SELECT 
        STRFTIME('%m', order_purchase_timestamp) AS month_no,
        CASE 
            WHEN STRFTIME('%m', order_purchase_timestamp) = '01' THEN 'Jan'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '02' THEN 'Feb'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '03' THEN 'Mar'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '04' THEN 'Apr'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '05' THEN 'May'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '06' THEN 'Jun'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '07' THEN 'Jul'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '08' THEN 'Aug'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '09' THEN 'Sep'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '10' THEN 'Oct'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '11' THEN 'Nov'
            WHEN STRFTIME('%m', order_purchase_timestamp) = '12' THEN 'Dec'
        END AS month,
        STRFTIME('%Y', order_purchase_timestamp) AS year,
        AVG(julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp)) AS real_time,
        AVG(julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp)) AS estimated_time
    FROM olist_orders
    WHERE order_status = 'delivered' 
        AND order_delivered_customer_date IS NOT NULL
    GROUP BY month_no, year
)
SELECT 
    month_no,
    month,
    MAX(CASE WHEN year = '2016' THEN real_time END) AS Year2016_real_time,
    MAX(CASE WHEN year = '2017' THEN real_time END) AS Year2017_real_time,
    MAX(CASE WHEN year = '2018' THEN real_time END) AS Year2018_real_time,
    MAX(CASE WHEN year = '2016' THEN estimated_time END) AS Year2016_estimated_time,
    MAX(CASE WHEN year = '2017' THEN estimated_time END) AS Year2017_estimated_time,
    MAX(CASE WHEN year = '2018' THEN estimated_time END) AS Year2018_estimated_time
FROM entrega_diferencias
GROUP BY month_no, month
ORDER BY month_no;
