WITH source AS (
    SELECT * FROM {{ source('ecommerce_source', 'orders') }}
),

renamed AS (

    SELECT
        order_id AS order_internal_id,
        customer_id AS customer_internal_id,
        order_status,
        CAST (order_purchase_timestamp AS TIMESTAMP) AS order_purchased_at,
        CAST (order_approved_at AS TIMESTAMP),
        CAST (order_delivered_carrier_date AS TIMESTAMP) AS order_shipped_at,
        CAST (order_delivered_customer_date AS TIMESTAMP) AS order_delivered_at,
        CAST (order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_at

    FROM source

)

SELECT * FROM renamed
