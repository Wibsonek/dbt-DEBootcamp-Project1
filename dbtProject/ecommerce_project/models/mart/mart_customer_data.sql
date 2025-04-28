WITH customer_data AS (

        SELECT * FROM {{ ref('int_customers_region_joined') }}

    ),

    order_data AS (

        SELECT * FROM {{ ref('int_order_data_joined') }}

    ),

    customer_orders_joined AS (

        SELECT 
            customer_data.customer_internal_id AS customer_internal_id,
            customer_data.customer_zip_prefix AS customer_zip_prefix,
            customer_data.customer_city_name AS customer_city_name,
            customer_data.customer_state_code AS customer_state_code,
            customer_data.region_name AS region_name,
            order_data.order_internal_id AS order_internal_id,
            order_data.order_status AS order_status,
            order_data.order_purchased_at AS order_purchased_at,
            order_data.order_delivered_at AS order_delivered_at,
            order_data.product_internal_id AS product_internal_id,
            order_data.order_item_number AS order_item_number,
            order_data.item_price AS item_price,
            order_data.shipping_cost AS shipping_cost,
            order_data.payment_type AS payment_type,
            order_data.payment_amount AS payment_amount


        FROM customer_data
        JOIN order_data 
        ON customer_data.customer_internal_id = order_data.customer_internal_id

    )

SELECT * FROM customer_orders_joined
