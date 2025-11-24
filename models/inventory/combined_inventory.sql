{{ config(
    materialized='table',
    schema='CLEAN',
    database='DEPLETIONS',
    alias='combined_inventory',
    tags=['depletions'],
    enabled=var('enable_depletions', false)
) }}

WITH src AS (
    -- Read the JSON from the stage as VARIANT
    SELECT
        t.$1::variant AS file_content
    FROM @DEPLETIONS.RAW.GCS_DEPLETIONS_LANDING_STAGE (FILE_FORMAT => 'ns_json_format') t
),

flattened AS (
    -- Turn the top-level array into one row per element
    SELECT
        value::variant AS json_row
    FROM src,
         LATERAL FLATTEN(input => file_content)
),

main_table AS (
    SELECT
        json_row:"Customer ID"::VARCHAR      AS customerid,
        json_row:"ITEM CODE"::VARCHAR        AS itemcode,
        json_row:"ITEM NAME"::VARCHAR        AS itemname,
        json_row:"PACK SIZE"::VARCHAR        AS packsize,
        json_row:"BOTTLE SIZE"::VARCHAR      AS bottlesize,
        json_row:"VINTAGE"::VARCHAR          AS vintage,
        json_row:"QUANTITY"::VARCHAR         AS quantity,
        json_row:"QUANTITY ON ORDER"::VARCHAR AS quantityonorder,
        json_row:"REPORT DATE"::VARCHAR      AS reportdate,
        json_row:"PRODUCER NAME"::VARCHAR    AS producername,
        json_row:"DATE_RANGE_START"::VARCHAR AS daterangestart,
        json_row:"DATE_RANGE_END"::VARCHAR   AS daterangeend,
        json_row:"TRANSFORM_APPLIED"::VARCHAR AS transformapplied,
        json_row:"DATE"::VARCHAR             AS date,
        json_row                             AS raw_data
    FROM flattened
)

SELECT 
    mt.*,
    m.vvg_wine_internal_id,
    m.obsolete,
    m.vvg_sku_code
FROM main_table AS mt
LEFT JOIN {{ ref('matching_table') }} AS m
    ON mt.itemcode = m.distributor_sku_code
   AND mt.customerid    = m.distributor_customer_id
