{{ config(materialized='table') }}

SELECT
    MD5(TRIM(ma.asset_kennung))::text AS "Id",
    COALESCE(TRIM(INITCAP(ma.asset_name)), TRIM(ma.asset_kennung))::text AS "Name",
    TRIM(ma.serien_nummer)::text AS "Serial_Number__c",
    CASE
        WHEN TRIM(ma.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(ma.garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(ma.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END::text AS "Warranty_End_Date__c",
    MD5(TRIM(ma.kunden_kennung))::text AS "Account__c",
    MD5(TRIM(ma.projekt_kennung))::text AS "Project__c",
    TRIM(ma.asset_kennung)::text AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
WHERE
    ma.asset_kennung IS NOT NULL