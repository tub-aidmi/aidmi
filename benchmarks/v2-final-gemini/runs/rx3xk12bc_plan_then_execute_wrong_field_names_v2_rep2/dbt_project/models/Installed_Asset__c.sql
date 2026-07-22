{{ config(materialized='table') }}

WITH stg_account_ids AS (
    -- Re-derives Account IDs to resolve foreign key for Account__c
    SELECT
        MD5(kunden.kunden_nr) AS account_id,
        kunden.kunden_nr AS legacy_customer_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
),
stg_project_ids AS (
    -- Re-derives Project IDs to resolve foreign key for Project__c
    SELECT
        MD5(proj.proj_id) AS project_id,
        proj.proj_id AS legacy_project_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
)

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unknown Asset') AS "Name",
    assets.seriennr AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN assets.garantie_bis
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    stg_account_ids.account_id AS "Account__c",
    stg_project_ids.project_id AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN
    stg_account_ids
    ON assets.kd_ref = stg_account_ids.legacy_customer_id
LEFT JOIN
    stg_project_ids
    ON assets.projekt_ref = stg_project_ids.legacy_project_id
