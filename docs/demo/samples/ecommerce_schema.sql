-- E-Commerce Data Warehouse Schema
-- Sample input for SEG Demo — demonstrates SQL DDL parsing

CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE ecommerce.customers (
    customer_id     BIGINT PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP,
    status          VARCHAR(20) CHECK (status IN ('active', 'inactive', 'suspended')),
    tier            VARCHAR(10) DEFAULT 'bronze' CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    lifetime_value  DECIMAL(12,2) DEFAULT 0.00
);

CREATE TABLE ecommerce.products (
    product_id      BIGINT PRIMARY KEY,
    sku             VARCHAR(50) NOT NULL UNIQUE,
    name            VARCHAR(300) NOT NULL,
    description     TEXT,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    unit_price      DECIMAL(10,2) NOT NULL,
    cost_price      DECIMAL(10,2),
    weight_kg       DECIMAL(8,3),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inventory_count INT DEFAULT 0
);

CREATE TABLE ecommerce.orders (
    order_id        BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES ecommerce.customers(customer_id),
    order_date      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status          VARCHAR(30) CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
    shipping_method VARCHAR(50),
    shipping_address_id BIGINT,
    subtotal        DECIMAL(12,2) NOT NULL,
    tax_amount      DECIMAL(10,2) DEFAULT 0.00,
    shipping_cost   DECIMAL(10,2) DEFAULT 0.00,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    total_amount    DECIMAL(12,2) NOT NULL,
    currency        VARCHAR(3) DEFAULT 'USD',
    notes           TEXT
);

CREATE TABLE ecommerce.order_items (
    item_id         BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL REFERENCES ecommerce.orders(order_id),
    product_id      BIGINT NOT NULL REFERENCES ecommerce.products(product_id),
    quantity        INT NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10,2) NOT NULL,
    discount_pct    DECIMAL(5,2) DEFAULT 0.00,
    line_total      DECIMAL(12,2) NOT NULL
);

CREATE TABLE ecommerce.addresses (
    address_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES ecommerce.customers(customer_id),
    address_type    VARCHAR(20) CHECK (address_type IN ('billing', 'shipping')),
    street_line1    VARCHAR(255) NOT NULL,
    street_line2    VARCHAR(255),
    city            VARCHAR(100) NOT NULL,
    state_province  VARCHAR(100),
    postal_code     VARCHAR(20),
    country_code    VARCHAR(3) NOT NULL,
    is_default      BOOLEAN DEFAULT FALSE
);

CREATE TABLE ecommerce.payments (
    payment_id      BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL REFERENCES ecommerce.orders(order_id),
    payment_method  VARCHAR(50) NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    currency        VARCHAR(3) DEFAULT 'USD',
    status          VARCHAR(20) CHECK (status IN ('pending', 'authorized', 'captured', 'failed', 'refunded')),
    transaction_ref VARCHAR(100),
    processed_at    TIMESTAMP
);

CREATE TABLE ecommerce.reviews (
    review_id       BIGINT PRIMARY KEY,
    product_id      BIGINT NOT NULL REFERENCES ecommerce.products(product_id),
    customer_id     BIGINT NOT NULL REFERENCES ecommerce.customers(customer_id),
    rating          INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title           VARCHAR(200),
    body            TEXT,
    is_verified     BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecommerce.inventory_events (
    event_id        BIGINT PRIMARY KEY,
    product_id      BIGINT NOT NULL REFERENCES ecommerce.products(product_id),
    event_type      VARCHAR(30) CHECK (event_type IN ('restock', 'sale', 'return', 'adjustment', 'writeoff')),
    quantity_change  INT NOT NULL,
    warehouse_id    VARCHAR(20),
    reference_id    VARCHAR(100),
    event_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
