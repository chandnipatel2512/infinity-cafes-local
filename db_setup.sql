CREATE TABLE product (
    product_id varchar(255) NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    type varchar(255),
    extras varchar(255),
    size varchar(255)
);

CREATE TABLE location (
    location_id varchar(255) NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL
);

CREATE TABLE transaction (
    transaction_id varchar(255) NOT NULL PRIMARY KEY,
    date_time timestamp NOT NULL,
    payment_type varchar(255) NOT NULL,
    total_cost decimal(5,2) NOT NULL,
    location_id varchar(255) NOT NULL REFERENCES location (location_id)
);

CREATE TABLE basket (
    basket_id varchar(255) NOT NULL PRIMARY KEY,
    transaction_id varchar(255) NOT NULL REFERENCES transaction (transaction_id),
    product_id varchar(255) NOT NULL REFERENCES product (product_id),
    price decimal(5,2) NOT NULL
);
