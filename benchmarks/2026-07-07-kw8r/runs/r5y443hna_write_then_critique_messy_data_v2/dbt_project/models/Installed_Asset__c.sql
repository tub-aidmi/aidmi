{{ config(materialized='table') }}

SELECT
    -- Id: Map directly from source id, coerce to text
    CAST(TRIM("id") AS text) AS "Id",

    -- Name: Ensure NOT NULL with a meaningful default when source is empty
    COALESCE(TRIM("name"), 'Unknown Asset') AS "Name",

    -- Serial_Number__c: Trim leading/trailing whitespace
    TRIM("serial_number__c") AS "Serial_Number__c",

    -- Warranty_End_Date__c: Parse multiple date formats into ISO YYYY-MM-DD; NULL on failure
    CASE
        WHEN "warranty_end_date__c" IS NULL OR TRIM("warranty_end_date__c") = '' THEN NULL
        WHEN TRIM("warranty_end_date__c") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM("warranty_end_date__c"), 'YYYY-MM-DD')::TEXT
        WHEN TRIM("warranty_end_date__c") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM("warranty_end_date__c"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM("warranty_end_date__c") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM("warranty_end_date__c"), 'DD.MM.YYYY')::TEXT
        WHEN TRIM("warranty_end_date__c") ~ '^\d{8}$' THEN TO_DATE(TRIM("warranty_end_date__c"), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: Foreign key to Account.Id using consistent Salesforce-style transform.
    -- Prepend '001' (standard SFDC Account prefix) and strip all non-numeric characters.
    '001' || REGEXP_REPLACE(TRIM("account__c"), '[^0-9]', '', 'g') AS "Account__c",

    -- Project__c: Foreign key to Project__c.Id using consistent Salesforce-style transform.
    -- Prepend '00I' (standard SFDC custom object prefix) and strip all non-numeric characters.
    -- Note: Project__c model must apply the same transform to its own Id for cross-table joins.
    '00I' || REGEXP_REPLACE(TRIM("project__c"), '[^0-9]', '', 'g') AS "Project__c",

    -- Legacy_Asset_ID__c: Populate from source id for row-level verification linkage
    TRIM("id") AS "Legacy_Asset_ID__c",

    -- CreatedDate / LastModifiedDate: Not present in the source table; default to NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: Default to false (0) since no delete flag exists in source
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}