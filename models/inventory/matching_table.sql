{{ config(
    materialized='table',
    schema='CLEAN',
    database='DEPLETIONS',
    alias='matching_table',
    tags=['depletions'],
    enabled=var('enable_depletions', false)
) }}

WITH main_table AS (
    SELECT
       t.$1::VARCHAR AS distributor_customer_id,
        t.$2::VARCHAR AS distributor,
        t.$3::VARCHAR AS distributor_sku_code,
        t.$4::VARCHAR AS vvg_sku_code,
        t.$5::VARCHAR AS distributor_inventory_producer_name,
        t.$6::VARCHAR AS distributor_inventory_item_name_wine_name,
        t.$7::VARCHAR AS vvg_wine_internal_id,
        t.$8::VARCHAR AS obsolete,
    FROM @DEPLETIONS.RAW.GCS_DEPLETIONS_LANDING_STAGE/matching_files (FILE_FORMAT => 'CSV_MATCHING_FILE_FMT') AS t
)

SELECT * FROM main_table