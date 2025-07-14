CREATE TABLE IF NOT EXISTS data_mart.dm_product_popularity (
    product_id UUID PRIMARY KEY,
    product_name TEXT,
    total_quantity_sold INTEGER,
    total_revenue NUMERIC(12,2),
    order_count INTEGER
);