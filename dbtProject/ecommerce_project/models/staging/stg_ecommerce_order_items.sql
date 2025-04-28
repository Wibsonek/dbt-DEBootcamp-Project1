WITH source AS (
    SELECT * FROM {{ source('ecommerce_source', 'order_items') }}
),

renamed AS (
    SELECT
        order_id AS order_internal_id,
        order_item_id AS order_item_number,
        product_id AS product_internal_id,
        seller_id AS seller_internal_id,
        CAST(shipping_limit_date AS DATE) AS shipping_deadline,
        price AS item_price,
        freight_value AS shipping_cost
    FROM source
)

SELECT * FROM renamed