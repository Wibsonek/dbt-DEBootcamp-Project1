WITH seller_data AS (

        SELECT * FROM {{ ref('int_seller_region_joined') }}

    ),

    order_data AS (

        SELECT * FROM {{ ref('int_order_data_joined') }}

    ),


    seller_orders_joined AS (

        SELECT 
            seller_data.seller_internal_id AS seller_internal_id,
            seller_data.seller_zip_prefix AS seller_zip_prefix,
            seller_data.seller_city_name AS seller_city_name,
            seller_data.seller_state_code AS seller_state_code,
            seller_data.region_name AS region_name,
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


        FROM seller_data
        JOIN order_data 
        ON seller_data.seller_internal_id = order_data.seller_internal_id

    )


    SELECT * FROM seller_orders_joined
