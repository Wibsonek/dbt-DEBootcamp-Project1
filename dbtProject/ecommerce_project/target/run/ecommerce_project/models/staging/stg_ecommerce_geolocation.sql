
  
  create view "ecommerce_project"."main"."stg_ecommerce_geolocation__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."geolocation"
)
SELECT * FROM source
  );
