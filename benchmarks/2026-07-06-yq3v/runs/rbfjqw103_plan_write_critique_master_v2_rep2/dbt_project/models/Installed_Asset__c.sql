{{ config(materialized='table') }}

SELECT
    gen_random_uuid() AS "Id",
    COALESCE(TRIM(assets.asset_name), 'Unnamed Asset') AS "Name",
    TRIM(assets.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN assets.garantieende ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_model."Id" AS "Account__c",
    project_model."Id" AS "Project__c",
    TRIM(assets.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }} AS assets
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON TRIM(assets.kunden_kennung) = TRIM(kunden.kundennummer)
LEFT JOIN {{ ref('Account') }} AS account_model
    ON TRIM(kunden.kundennummer) = account_model."Legacy_Customer_ID__c"
LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} AS projekte
    ON TRIM(assets.projekt_kennung) = TRIM(projekte.projekt_kennung)
LEFT JOIN {{ ref('Project__c') }} AS project_model
    ON TRIM(projekte.projekt_kennung) = project_model."Legacy_Project_ID__c"