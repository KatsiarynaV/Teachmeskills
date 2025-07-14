INSERT INTO data_mart.dm_product_popularity (
    product_id,
    product_name,
    total_quantity_sold,
    total_revenue,
    order_count
)
SELECT 
    p.product_id,
    p.product_name,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.total_price) as total_revenue,
    COUNT(DISTINCT oi.order_id) as order_count
FROM dds.order_items oi
JOIN dds.products p on oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ON CONFLICT (product_id)
DO UPDATE SET
    product_name = EXCLUDED.product_name,
    total_quantity_sold = EXCLUDED.total_quantity_sold,
    total_revenue = EXCLUDED.total_revenue,
    order_count = EXCLUDED.order_count;

