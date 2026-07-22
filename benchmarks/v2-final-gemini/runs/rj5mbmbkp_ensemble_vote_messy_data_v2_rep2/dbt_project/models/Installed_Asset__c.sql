-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.id AS "Id",
    COALESCE({{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.name, 'Unknown Asset') AS "Name",
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.serial_number__c AS "Serial_Number__c",
    COALESCE(
        CASE WHEN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
             THEN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c
             ELSE NULL END,
        CASE WHEN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$'
             THEN TO_CHAR(TO_DATE({{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
             ELSE NULL END,
        CASE WHEN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c ~ '^\d{4}\/\d{2}\/\d{2}$'
             THEN TO_CHAR(TO_DATE({{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c, 'YYYY/MM/DD'), 'YYYY-MM-DD')
             ELSE NULL END,
        CASE WHEN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c ~ '^\d{2}\/\d{2}\/\d{4}$'
             THEN TO_CHAR(TO_DATE({{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
             ELSE NULL END
    ) AS "Warranty_End_Date__c",
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.account__c AS "Account__c",
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.project__c AS "Project__c",
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}