-- This dbt model transforms master_assets data into the Installed_Asset__c target schema.

{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS "Id",
    COALESCE(ma.asset_name, ma.asset_kennung) AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Generate Account__c Id based on the assumption that Account.Id is 'ACC_' || kundennummer
    'ACC_' || mk.kundennummer AS "Account__c",
    -- Generate Project__c Id based on the assumption that Project__c.Id is 'PRJ_' || projekt_kennung
    'PRJ_' || mp.projekt_kennung AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON ma.projekt_kennung = mp.projekt_kennung