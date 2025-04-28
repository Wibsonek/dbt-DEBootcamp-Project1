WITH customers AS (
        SELECT * FROM {{ ref('stg_ecommerce_customers') }}
),

    regions AS (
        SELECT * FROM  {{ ref('Region_Mapping') }}
    ),

    joined_data AS (
        SELECT
            customers.customer_internal_id AS customer_internal_id,
            customers.customer_uuid AS customer_uuid,
            customers.customer_zip_prefix AS customer_zip_prefix,
            customers.customer_city_name AS customer_city_name,
            customers.customer_state_code AS customer_state_code,
            regions.region_name AS region_name

        FROM customers
        JOIN regions
        ON customers.customer_state_code = regions.state_code
    )

SELECT * FROM joined_data