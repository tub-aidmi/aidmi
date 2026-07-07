-- depends_on: {{ ref('Account') }} {{ ref('Project__c') }}
{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, 'Unknown') AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis IS NULL THEN NULL
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        -- Add more date formats if known, e.g., for DD.MM.YYYY:
        -- WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL -- Fallback for unparseable formats
    END AS "Warranty_End_Date__c",
    MD5(k.kunden_nr || k.firmenname) AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON a.projekt_ref = p.proj_id