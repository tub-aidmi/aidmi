-- depends_on: {{ ref('Account') }} -- depends_on: {{ ref('Project__c') }}
{{ config(materialized='table') }}

SELECT
    'AST_' || assets.asset_id AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    assets.garantie_bis AS "Warranty_End_Date__c",
    'ACC_' || assets.kd_ref AS "Account__c",
    'PRJ_' || assets.projekt_ref AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets