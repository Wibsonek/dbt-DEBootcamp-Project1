
  
  create view "ecommerce_project"."main"."int_orders_joined__dbt_tmp" as (
    WITH order_items AS (

    SELECT * FROM "ecommerce_project"."main"."stg_ecommerce_order_items"

),

products AS (

    SELECT * FROM "ecommerce_project"."main"."stg_ecommerce_products"

),

orders AS (

    SELECT * FROM "ecommerce_project"."main"."stg_ecommerce_orders"

),

joined AS (

    SELECT 
        order_items.order_internal_id,
        customer_internal_id,
        item_price,
        product_category
    FROM order_items
    LEFT JOIN products
        ON order_items.product_internal_id = products.product_internal_id
    LEFT JOIN orders
        ON order_items.order_internal_id = orders.order_internal_id
)


SELECT * FROM joined
  );
