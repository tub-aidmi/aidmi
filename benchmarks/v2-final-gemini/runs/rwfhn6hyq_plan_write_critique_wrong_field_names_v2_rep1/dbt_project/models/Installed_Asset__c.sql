{{ config(materialized='table') }}

SELECT
    TRIM(a.asset_id) AS "Id",
    COALESCE(TRIM(a.bezeichnung), 'Unknown Asset Name') AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(k.kunden_nr) AS "Account__c",
    TRIM(p.proj_id) AS "Project__c",
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)
