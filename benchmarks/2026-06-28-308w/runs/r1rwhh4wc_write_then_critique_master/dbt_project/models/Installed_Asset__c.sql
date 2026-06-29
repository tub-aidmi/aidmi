
{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS "Id",
    COALESCE(TRIM(ma.asset_name), TRIM(ma.asset_kennung)) AS "Name",
    TRIM(ma.serien_nummer) AS "Serial_Number__c",
    CASE
        -- Handle sentinel date '0000-00-00' first
        WHEN ma.garantieende = '0000-00-00' THEN NULL

        -- Format: YYYY-MM-DD (already in desired output format)
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende

        -- Format: YYYYMMDD
        WHEN ma.garantieende ~ '^\d{8}$' AND
             CAST(SUBSTRING(ma.garantieende FROM 5 FOR 2) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SUBSTRING(ma.garantieende FROM 7 FOR 2) AS INTEGER) BETWEEN 1 AND 31
        THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')

        -- Format: DD.MM.YYYY
        WHEN ma.garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' AND
             CAST(SPLIT_PART(ma.garantieende, '.', 2) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SPLIT_PART(ma.garantieende, '.', 1) AS INTEGER) BETWEEN 1 AND 31
        THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')

        -- Format: MM/DD/YYYY
        WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND
             CAST(SPLIT_PART(ma.garantieende, '/', 1) AS INTEGER) BETWEEN 1 AND 12 AND
             CAST(SPLIT_PART(ma.garantieende, '/', 2) AS INTEGER) BETWEEN 1 AND 31
        THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')

        -- Default to NULL if no format matches or basic validation fails
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ma.kunden_kennung AS "Account__c",
    ma.projekt_kennung AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_assets') }} AS ma
