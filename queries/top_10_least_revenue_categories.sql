-- TODO: Esta consulta devolverá una tabla con las 10 categorías con menores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con menores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.
WITH mapped_categories AS (
  SELECT
    CASE 
      WHEN p.product_category_name = 'seguros_e_servicos' THEN 'security_and_services'
      WHEN p.product_category_name = 'fashion_roupa_infanto_juvenil' THEN 'fashion_childrens_clothes'
      WHEN p.product_category_name = 'cds_dvds_musicais' THEN 'cds_dvds_musicals'
      WHEN p.product_category_name = 'casa_conforto_2' THEN 'home_comfort_2'
      WHEN p.product_category_name = 'flores' THEN 'flowers'
      WHEN p.product_category_name = 'artes_e_artesanato' THEN 'arts_and_craftmanship'
      WHEN p.product_category_name = 'la_cuisine' THEN 'la_cuisine'
      WHEN p.product_category_name = 'fashion_esporte' THEN 'fashion_sport'
      WHEN p.product_category_name = 'fraldas_higiene' THEN 'diapers_and_hygiene'
      WHEN p.product_category_name = 'fashion_roupa_feminina' THEN 'fashio_female_clothing'
    END AS Category,
    o.order_id,
    py.payment_value
  FROM olist_orders AS o
  JOIN olist_order_payments AS py ON o.order_id = py.order_id
  JOIN olist_order_items AS oi ON o.order_id = oi.order_id
  JOIN olist_products AS p ON oi.product_id = p.product_id
  WHERE o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND p.product_category_name IS NOT NULL
)
SELECT
  Category,
  COUNT(DISTINCT order_id) AS Num_order,
  SUM(payment_value) AS Revenue
FROM mapped_categories
WHERE Category IS NOT NULL 
GROUP BY Category
ORDER BY Revenue ASC
LIMIT 10;