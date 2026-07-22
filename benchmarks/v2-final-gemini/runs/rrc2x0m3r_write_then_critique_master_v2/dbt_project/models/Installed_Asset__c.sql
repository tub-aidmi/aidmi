-- depends_on: {{ ref('Account') }} {{ ref('Project__c') }}
{{ config(materialized='table') }}

SELECT
    MD5(asset.asset_kennung) AS "Id",
    COALESCE(asset.asset_name, asset.asset_kennung) AS "Name",
    asset.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN asset.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.garantieende
        WHEN asset.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc."Id" AS "Account__c",
    proj."Id" AS "Project__c",
    asset.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS asset
LEFT JOIN
    {{ ref('Account') }} AS acc
    ON asset.kunden_kennung = acc."Legacy_Customer_ID__c"
LEFT JOIN
    {{ ref('Project__c') }} AS proj
    ON asset.projekt_kennung = proj."Legacy_Project_ID__c"