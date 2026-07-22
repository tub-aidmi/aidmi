{{ config(materialized='table') }}

SELECT
    ia.id AS "Id",
    COALESCE(TRIM(ia.name), 'Unknown Asset') AS "Name",
    TRIM(ia.serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN ia.warranty_end_date__c IS NULL THEN NULL
        WHEN ia.warranty_end_date__c = '0000-00-00' THEN NULL
        WHEN ia.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN ia.warranty_end_date__c
        WHEN ia.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ia.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ia.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(ia.account__c) AS "Account__c",
    TRIM(ia.project__c) AS "Project__c",
    ia.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS ia