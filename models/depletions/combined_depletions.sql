{{ config(
    materialized='table',
    schema='CLEAN',
    database='DEPLETIONS',
    alias='combined_depletions',
    tags=['depletions'],
    enabled=var('enable_depletions', false)
) }}

WITH main_table AS (
    SELECT
        json_data:"Customer ID"::VARCHAR AS customerid,
        json_data:"ACCOUNT CODE"::VARCHAR AS accountcode,
        json_data:"ACCOUNT NAME"::VARCHAR AS accountname,
        json_data:"ITEM CODE"::VARCHAR AS itemcode,
        json_data:"ITEM NAME"::VARCHAR AS itemname,
        json_data:"PACK SIZE"::VARCHAR AS packsize,
        json_data:"BOTTLE SIZE"::VARCHAR AS bottlesize,
        json_data:"VINTAGE"::VARCHAR AS vintage,
        json_data:"DATE"::VARCHAR AS date,
        json_data:"QUANTITY"::VARCHAR AS quantity,
        json_data:"CASE QTY"::VARCHAR AS caseqty,
        json_data:"SALES VALUE"::VARCHAR AS salesvalue,
        json_data:"INVOICE #"::VARCHAR AS invoice,
        json_data:"SALES REP NAME"::VARCHAR AS salesrepname,
        json_data:"SALES REP CODE"::VARCHAR AS salesrepcode,
        json_data:"ADDRESS 1"::VARCHAR AS address1,
        json_data:"ADDRESS 2"::VARCHAR AS address2,
        json_data:"CITY"::VARCHAR AS city,
        json_data:"STATE"::VARCHAR AS state,
        json_data:"ZIP"::VARCHAR AS zip,
        json_data:"PREMISE"::VARCHAR AS premise,
        json_data:"PRODUCER NAME"::VARCHAR AS producername,
        json_data:"DATE_RANGE_START"::VARCHAR AS daterangestart,
        json_data:"DATE_RANGE_END"::VARCHAR AS daterangeend,
        json_data:"TRANSFORM_APPLIED"::VARCHAR AS transformapplied,
        json_data as raw_data,
    FROM @DEPLETIONS.RAW.GCS_DEPLETIONS_LANDING_STAGE (FILE_FORMAT => 'NS_JSON_FORMAT') t(json_data)
)

SELECT * FROM main_table
