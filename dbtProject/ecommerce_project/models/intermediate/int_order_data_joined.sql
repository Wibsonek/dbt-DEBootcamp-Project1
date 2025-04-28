WITH payments AS (
        SELECT * FROM {{ ref('stg_ecommerce_payments') }}
),

    orders AS (
        SELECT * FROM {{ ref('stg_ecommerce_orders') }}
    ),

    order_items AS (
        SELECT * FROM {{ ref('stg_ecommerce_order_items') }}
    ),


    joined_data AS (

        SELECT 
            payments.order_internal_id AS order_internal_id,
            payments.payment_type AS payment_type,
            payments.payment_amount AS payment_amount,
            orders.customer_internal_id AS customer_internal_id,
            order_items.seller_internal_id AS seller_internal_id,
            orders.order_status AS order_status,
            orders.order_purchased_at AS order_purchased_at,
            orders.order_delivered_at AS order_delivered_at,
            order_items.product_internal_id AS product_internal_id,
            order_items.order_item_number AS order_item_number,
            order_items.item_price AS item_price,
            order_items.shipping_cost AS shipping_cost

        FROM payments
        JOIN orders
            ON payments.order_internal_id = orders.order_internal_id
        JOIN order_items 
            ON payments.order_internal_id = order_items.order_internal_id

    )

SELECT * FROM joined_data

