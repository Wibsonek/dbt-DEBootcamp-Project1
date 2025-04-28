WITH source AS (
    SELECT * FROM {{ source('ecommerce_source', 'geolocation') }}
)
SELECT * FROM source
