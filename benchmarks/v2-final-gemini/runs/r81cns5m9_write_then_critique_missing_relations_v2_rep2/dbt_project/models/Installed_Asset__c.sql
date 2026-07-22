{{ config(materialized='table') }}

WITH parsed_dates AS (
    SELECT
        id,
        name,
        serial,
        client,
        project,
        -- Attempt to parse common date formats for warranty
        CASE
            WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD') -- YYYY-MM-DD
            WHEN warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            WHEN warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(warranty, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            ELSE NULL
        END AS warranty_parsed_date
    FROM
        {{ source('fixture_missing_relations_v2_src', 'asset') }}
)
SELECT
    asset.id AS "Id",
    COALESCE(asset.name, 'Unknown Asset') AS "Name",
    asset.serial AS "Serial_Number__c",
    parsed_dates.warranty_parsed_date AS "Warranty_End_Date__c",
    -- Join to source account to get the Salesforce-style Account Id
    account.id AS "Account__c",
    -- Join to source project to get the Salesforce-style Project Id
    project.id AS "Project__c",
    asset.id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
JOIN parsed_dates ON asset.id = parsed_dates.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} AS account
    ON asset.client = account.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON asset.project = project.id
