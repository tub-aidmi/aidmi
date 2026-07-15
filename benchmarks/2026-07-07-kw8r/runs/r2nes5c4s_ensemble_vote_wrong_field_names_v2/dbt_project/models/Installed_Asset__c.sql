{{ config(materialized='table') }}

SELECT
    SUBSTR(MD5(a.asset_id), 1, 18) AS "Id",
    INITCAP(TRIM(a.bezeichnung)) AS "Name",
    a.seriennr AS "Serial_Number__c",
    a.garantie_bis AS "Warranty_End_Date__c",
    SUBSTR(MD5(k.kunden_nr), 1, 18) AS "Account__c",
    SUBSTR(MD5(p.proj_id), 1, 18) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON a.projekt_ref = p.proj_id