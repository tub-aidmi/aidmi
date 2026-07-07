{{ config(materialized='table') }}

SELECT
    s.asset_id AS "Id",
    COALESCE(s.bezeichnung, 'Unknown Asset') AS "Name",
    s.seriennr AS "Serial_Number__c",
    CASE
        WHEN s.garantie_bis ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN s.garantie_bis -- YYYY-MM-DD
        WHEN s.garantie_bis ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_CHAR(TO_DATE(s.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN s.garantie_bis ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_CHAR(TO_DATE(s.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c", -- This links to the legacy customer ID, not a Salesforce Account Id. Further mapping would be needed.
    p.proj_id AS "Project__c",
    s.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS s
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON s.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON s.projekt_ref = p.proj_id
