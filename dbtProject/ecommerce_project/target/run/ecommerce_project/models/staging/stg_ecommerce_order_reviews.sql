
  
  create view "ecommerce_project"."main"."stg_ecommerce_order_reviews__dbt_tmp" as (
    WITH source AS (
    SELECT * FROM "ecommerce_project"."main"."order_reviews"
),

renamed AS (
    SELECT
        review_id AS review_internal_id,
        order_id AS order_internal_id,
        review_score AS review_rating,
        review_comment_title AS review_title,
        review_comment_message AS review_message,
        CAST(review_creation_date AS DATE) AS review_created_date,
        CAST (review_answer_timestamp AS TIMESTAMP) AS review_answered_date
    FROM source

)

SELECT * FROM renamed
  );
