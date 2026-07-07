-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

WITH cleaned_assets AS (
    SELECT
        id,
        name,
        serial_number__c,
        CASE
            WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS cleaned_warranty_end_date,
        account__c,
        project__c
    FROM
        {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)
SELECT
    ca.id AS "Id",
    COALESCE(ca.name, 'Unknown Asset') AS "Name",
    ca.serial_number__c AS "Serial_Number__c",
    ca.cleaned_warranty_end_date AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    ca.project__c AS "Project__c",
    ca.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_assets ca
LEFT JOIN
    {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON ca.account__c = a.legacy_customer_id__c