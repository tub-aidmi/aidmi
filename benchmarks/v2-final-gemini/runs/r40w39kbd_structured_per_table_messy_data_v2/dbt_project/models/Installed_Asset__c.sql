{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(CAST(name AS TEXT), 'Unknown Asset') AS "Name",
    CAST(serial_number__c AS TEXT) AS "Serial_Number__c",
    CAST(warranty_end_date__c AS TEXT) AS "Warranty_End_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(project__c AS TEXT) AS "Project__c",
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
