CREATE SCHEMA sales_XXX;


CREATE TABLE sales_XXX.products_XXX(
retailer_country  varchar(20),
order_method_type varchar(15),
retailer_type  varchar(30),
product_line varchar(30),
product_type varchar(30),
product varchar(50),
year varchar(4),
revenue numeric(15,2),
quantity integer,
gross_margin  numeric(15,10),
profit    numeric(15,2),
timestamp    date
);

SELECT * FROM sales_XXX.products_XXX LIMIT 50;
