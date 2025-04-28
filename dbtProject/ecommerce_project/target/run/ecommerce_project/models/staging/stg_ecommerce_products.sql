
  
  create view "ecommerce_project"."main"."stg_ecommerce_products__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."products"
),

renamed AS (
    SELECT 
        product_id AS product_internal_id,
        product_category_name AS product_category,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty AS product_image_count,
        product_weight_g AS product_weight_grams,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM source
)

SELECT * FROM renamed
  );
