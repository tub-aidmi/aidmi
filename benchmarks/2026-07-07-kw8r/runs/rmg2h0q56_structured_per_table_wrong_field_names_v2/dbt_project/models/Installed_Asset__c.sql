{{ config(materialized='table') }}

WITH account_key_map AS (
    SELECT 
        kunden_nr,
        '001' || LOWER(LEFT(MD5(kunden_nr), 17)) as sf_account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
project_key_map AS (
    SELECT 
        proj_id,
        'a0B' || LOWER(LEFT(MD5(proj_id), 17)) as sf_project_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    a.asset_id                                          AS "Id",
    INITCAP(TRIM(a.bezeichnung))                         AS "Name",
    TRIM(a.seriennr)                                     AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$'   THEN TO_DATE(a.garantie_bis, 'YYYY-MM-DD')::TEXT
        WHEN a.garantie_bis ~ '^\d{8}$'                 THEN TO_DATE(a.garantie_bis, 'YYYYMMDD')::TEXT
        WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$'    THEN TO_DATE(a.garantie_bis, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END                                                  AS "Warranty_End_Date__c",
    ak.sf_account_id                                     AS "Account__c",
    pk.sf_project_id                                     AS "Project__c",
    a.asset_id                                           AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT                              AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT                              AS "LastModifiedDate",
    0                                                    AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN account_key_map ak ON a.kd_ref = ak.kunden_nr
LEFT JOIN project_key_map pk ON a.projekt_ref = pk.proj_id