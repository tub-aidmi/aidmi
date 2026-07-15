{{ config(materialized='table') }}

SELECT
    CAST(a.id AS TEXT) AS "Id",
    CAST(a.name AS TEXT) AS "Name",
    CAST(a.serial_number__c AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN a.warranty_end_date__c IS NULL THEN NULL
        WHEN a.warranty_end_date__c ~ '^\d{8}$' THEN
            SUBSTRING(a.warranty_end_date__c, 1, 4) || '-' ||
            SUBSTRING(a.warranty_end_date__c, 5, 2) || '-' ||
            SUBSTRING(a.warranty_end_date__c, 7, 2)
        WHEN a.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(a.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(a.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND a.warranty_end_date__c != '0000-00-00' THEN
            a.warranty_end_date__c
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acct."Id" AS "Account__c",
    proj."Id" AS "Project__c",
    CAST(a.id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} a
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acct
    ON a.account__c = acct.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} proj
    ON a.project__c = proj.id