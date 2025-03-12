-- TODO: Esta consulta devolverá una tabla con las 10 categorías con mayores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con mayores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.
SELECT 
    CASE 
      WHEN p.product_category_name = 'cama_mesa_banho' THEN 'bed_bath_table'
      WHEN p.product_category_name = 'beleza_saude' THEN 'health_beauty'
      WHEN p.product_category_name = 'informatica_acessorios' THEN 'computers_accessories'
      WHEN p.product_category_name = 'moveis_decoracao' THEN 'furniture_decor'
      WHEN p.product_category_name = 'relogios_presentes' THEN 'watches_gifts'
      WHEN p.product_category_name = 'esporte_lazer' THEN 'sports_leisure'
      WHEN p.product_category_name = 'utilidades_domesticas' THEN 'housewares'
      WHEN p.product_category_name = 'automotivo' THEN 'auto'
      WHEN p.product_category_name = 'ferramentas_jardim' THEN 'garden_tools'
      WHEN p.product_category_name = 'cool_stuff' THEN 'cool_stuff'
      ELSE p.product_category_name
    END AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    SUM(py.payment_value) AS Revenue
FROM olist_orders AS o
JOIN olist_order_payments AS py ON o.order_id = py.order_id
JOIN olist_order_items AS oi ON o.order_id = oi.order_id
JOIN olist_products AS p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered' 
  AND o.order_delivered_customer_date IS NOT NULL
  AND p.product_category_name IS NOT NULL
GROUP BY Category
ORDER BY Revenue DESC
LIMIT 10;
