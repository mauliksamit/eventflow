-- Source system: a normalized (3NF) relational schema for an online store.
-- Each fact lives in exactly one place (no repeated product names on order lines,
-- no customer city on orders), which keeps writes cheap and consistent.
-- Analytics queries against it need many joins; that is why we build a separate
-- dimensional warehouse (see dw_schema.sql).
CREATE SCHEMA IF NOT EXISTS oltp;

DROP TABLE IF EXISTS oltp.order_items, oltp.orders, oltp.products, oltp.categories, oltp.customers CASCADE;

CREATE TABLE oltp.customers (
    customer_id   BIGINT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    city          TEXT NOT NULL,
    signup_date   DATE NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oltp.categories (
    category_id   INT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE
);

CREATE TABLE oltp.products (
    product_id    BIGINT PRIMARY KEY,
    category_id   INT NOT NULL REFERENCES oltp.categories(category_id),
    name          TEXT NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oltp.orders (
    order_id      BIGINT PRIMARY KEY,
    customer_id   BIGINT NOT NULL REFERENCES oltp.customers(customer_id),
    status        TEXT NOT NULL CHECK (status IN ('placed', 'shipped', 'cancelled')),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE oltp.order_items (
    order_id      BIGINT NOT NULL REFERENCES oltp.orders(order_id),
    line_no       INT NOT NULL,
    product_id    BIGINT NOT NULL REFERENCES oltp.products(product_id),
    quantity      INT NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10, 2) NOT NULL,  -- price at time of purchase (can differ from products.unit_price)
    PRIMARY KEY (order_id, line_no)
);

-- Supports the incremental extract (WHERE updated_at > last watermark).
CREATE INDEX ON oltp.orders (updated_at);
