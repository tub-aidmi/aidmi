{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        a.asset_id,
        a.bezeichnung,
        a.seriennr,
        a.garantie_bis,
        a.kd_ref,
        a.projekt_ref,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
),

account_ids AS (
    SELECT
        kunden_nr,
        '001' || SUBSTRING(MD5(kunden_nr), 1, 15) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

project_ids AS (
    SELECT
        proj_id,
        'a00' || SUBSTRING(MD5(proj_id), 1, 15) AS project_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    'a01' || SUBSTRING(MD5(ad.asset_id), 1, 15) AS "Id",
    ad.bezeichnung AS "Name",
    ad.seriennr AS "Serial_Number__c",
    CASE 
        WHEN ad.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN ad.garantie_bis
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ai.account_id AS "Account__c",
    pi.project_id AS "Project__c",
    ad.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data ad
LEFT JOIN account_ids ai ON ad.kd_ref = ai.kunden_nr
LEFT JOIN project_ids pi ON ad.projekt_ref = pi.proj_id
