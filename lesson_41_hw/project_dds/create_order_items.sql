CREATE TABLE IF NOT EXISTS dds.order_items (
    item_id UUID PRIMARY KEY,
    quantity INTEGER,
    price NUMERIC(10,2),
    total_price NUMERIC(10,2),
    order_id UUID REFERENCES dds.orders(order_id),
    product_id UUID REFERENCES dds.products(product_id)
);