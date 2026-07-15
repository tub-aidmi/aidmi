{{ config(materialized='table') }}

SELECT
    -- Id: generate Salesforce-style 18-character ID from source asset id
    UPPER(SUBSTRING(MD5(a.id || '|' || a.id), 1, 15))
        || RPAD(
            SUBSTRING('0123456789ABCDEF' FROM (MOD((SUBSTRING(MD5(a.id || '|' || a.id), 16, 1)::INTEGER + 1) / 6 % 2), 1)::INTEGER + 1, 1)
            || SUBSTRING('0123456789ABCDEF' FROM (MOD((SUBSTRING(MD5(a.id || '|' || a.id), 17, 1)::INTEGER + 1) / 6 % 2), 1)::INTEGER + 1, 1)
            || SUBSTRING('0123456789ABCDEF' FROM (MOD((SUBSTRING(MD5(a.id || '|' || a.id), 18, 1)::INTEGER + 1) / 6 % 2), 1)::INTEGER + 1, 1)
            || SUBSTRING('0123456789ABCDEF' FROM (MOD((SUBSTRING(MD5(a.id || '|' || a.id), 19, 1)::INTEGER + 1) / 6 % 2), 1)::INTEGER + 1, 1)
            || SUBSTRING('0123456789ABCDEF' FROM (MOD((SUBSTRING(MD5(a.id || '|' || a.id), 20, 1)::INTEGER + 1) / 6 % 2), 1)::INTEGER + 1, 1)
          , 3, 'A') AS "Id",

    -- Name: from asset.name, default to serial if name is null
    COALESCE(LOWER(a.name), LOWER(a.serial)) AS "Name",

    -- Serial_Number__c
    a.serial AS "Serial_Number__c",

    -- Warranty_End_Date__c: parse warranty date string safely
    CASE
        WHEN a.warranty IS NULL OR TRIM(a.warranty) = '' THEN NULL
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.warranty, 'YYYY-MM-DD')::TEXT
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.warranty, 'DD.MM.YYYY')::TEXT
        WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(a.warranty, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: reference Salesforce-style Account Id from joined account record
    ac.id AS "Account__c",

    -- Project__c: reference source project id (or null if no match)
    p.id AS "Project__c",

    -- Legacy_Asset_ID__c: preserve the source asset natural key
    a.id AS "Legacy_Asset_ID__c",

    -- CreatedDate / LastModifiedDate: use today as placeholder since source has no timestamps
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    -- IsDeleted: 0 = not deleted by default
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac
    ON a.client = ac.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON a.project = p.id
