
  
  create view "ecommerce_project"."main"."stg_ecommerce_sellers__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."sellers"
),

renamed AS (
    SELECT
        seller_id AS seller_internal_id,
        seller_zip_code_prefix  AS seller_zip_prefix,
        seller_city AS seller_city_name,
        seller_state  AS seller_state_code,
        CASE  -- to powinno leciec w intermediate po seed
                WHEN seller_state in ('PR', 'SC', 'RS') THEN 'South'
                WHEN seller_state in ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
                WHEN seller_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
                WHEN seller_state in ('DF', 'GO', 'MT', 'MS') THEN 'Central-West'
                WHEN seller_state in ('AM', 'PA', 'RO', 'RR', 'AC', 'AP', 'TO') THEN 'North'
                ELSE 'Undefined'
            END AS seller_region_name
    FROM source
)


SELECT * FROM renamed
  );
