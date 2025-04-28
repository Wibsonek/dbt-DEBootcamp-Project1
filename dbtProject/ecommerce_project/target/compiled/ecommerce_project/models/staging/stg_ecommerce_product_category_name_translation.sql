WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."product_category_name_translation"
),

renamed AS (
    SELECT
        product_category_name AS product_category_name_original,
        product_category_name_english
    FROM source
)

SELECT * FROM renamed