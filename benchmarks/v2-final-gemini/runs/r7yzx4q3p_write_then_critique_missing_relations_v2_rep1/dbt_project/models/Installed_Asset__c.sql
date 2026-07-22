-- depends_on: {{ ref('Account') }}
-- depends_on: {{ ref('Project__c') }}

{{ config(materialized='table') }}

SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset Name') AS "Name",
    asset.serial AS "Serial_Number__c",
    CASE
        WHEN asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(asset.warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_model."Id" AS "Account__c",
    project_model."Id" AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
LEFT JOIN
    {{ ref('Account') }} AS account_model
ON
    asset.client = account_model."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Project__c') }} AS project_model
ON
    asset.project = project_model."Legacy_Project_ID__c";