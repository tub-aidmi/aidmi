
{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, 'Unknown Asset') AS "Name",
    a.seriennr AS "Serial_Number__c",
    a.garantie_bis AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'kunden') }} AS k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_src', 'proj') }} AS p
    ON a.projekt_ref = p.proj_id
