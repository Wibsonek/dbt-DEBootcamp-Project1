WITH sellers AS (
        SELECT * FROM "ecommerce_project"."main"."stg_ecommerce_sellers"
),

    regions AS (
        SELECT * FROM  "ecommerce_project"."main"."Region_Mapping"
    ),

    joined_data AS (
        SELECT
            sellers.seller_internal_id AS seller_internal_id,
            sellers.seller_zip_prefix AS seller_zip_prefix,
            sellers.seller_city_name AS seller_city_name,
            sellers.seller_state_code AS seller_state_code,
            regions.region_name AS region_name

        FROM sellers
        JOIN regions
        ON sellers.seller_state_code = regions.state_code
    )

SELECT * FROM joined_data