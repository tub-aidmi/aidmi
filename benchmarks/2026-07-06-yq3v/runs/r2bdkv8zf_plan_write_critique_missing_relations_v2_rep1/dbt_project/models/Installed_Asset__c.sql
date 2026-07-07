{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(TRIM(a.name), 'Unknown Asset') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(TO_DATE(a.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(a.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(a.warranty, 'DD-MM-YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(a.warranty, 'YYYYMMDD'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    Account."Id" AS "Account__c",
    Project__c."Id" AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ ref('Account') }} AS Account
ON
    a.client = Account."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Project__c') }} AS Project__c
ON
    a.project = Project__c."Legacy_Project_ID__c"
