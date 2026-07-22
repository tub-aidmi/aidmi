-- depends_on: {{ ref('Account') }} -- This model ideally depends on Account and Project for FKs, but per rules, not used in initial write.
-- depends_on: {{ ref('Project__c') }}

{{ config(materialized='table') }}

SELECT
    CONCAT('IA-', a.asset_kennung) AS "Id",
    COALESCE(a.asset_name, a.asset_kennung) AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(a.garantieende AS DATE), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE WHEN k.kundennummer IS NOT NULL THEN CONCAT('ACC-', k.kundennummer) ELSE NULL END AS "Account__c",
    CASE WHEN p.projekt_kennung IS NOT NULL THEN CONCAT('PROJ-', p.projekt_kennung) ELSE NULL END AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS a
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON a.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
    ON a.projekt_kennung = p.projekt_kennung