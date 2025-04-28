WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."customers"
),

renamed AS (
    SELECT 
        customer_id AS customer_internal_id,
        customer_unique_id AS customer_uuid,
        customer_zip_code_prefix AS customer_zip_prefix,
        customer_city AS customer_city_name,
        customer_state AS customer_state_code,
        CASE 
            WHEN customer_state in ('PR', 'SC', 'RS') THEN 'South'
            WHEN customer_state in ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
            WHEN customer_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
            WHEN customer_state in ('DF', 'GO', 'MT', 'MS') THEN 'Central-West'
            WHEN customer_state in ('AM', 'PA', 'RO', 'RR', 'AC', 'AP', 'TO') THEN 'North'
            ELSE 'Undefined'
        END AS customer_region_name
    FROM source
)

SELECT * FROM renamed