WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."payments"
),

renamed AS (
    SELECT
        order_id AS order_internal_id,
        payment_sequential AS payment_sequence_number,
        payment_type,
        payment_installments AS installment_count,
        TRY_CAST(payment_value AS FLOAT) AS payment_amount
    FROM source
)

SELECT * FROM renamed