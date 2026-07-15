{{ config(materialized='table') }}

SELECT
    CAST(a.asset_id AS TEXT) AS "Id",
    INITCAP(CAST(a.bezeichnung AS TEXT)) AS "Name",
    CAST(a.seriennr AS TEXT) AS "Serial_Number__c",
    CAST(a.garantie_bis AS TEXT) AS "Warranty_End_Date__c",
    CAST(k.kunden_nr AS TEXT) AS "Account__c",
    CAST(p.proj_id AS TEXT) AS "Project__c",
    CAST(a.asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id