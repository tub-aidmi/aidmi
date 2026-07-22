SELECT 
    CAST("Id" AS text) AS "Id",
    COALESCE(TRIM("FirstName"), '') AS "FirstName",
    COALESCE(TRIM("LastName"), '') AS "LastName",
    LOWER(TRIM(COALESCE("Email", ''))) AS "Email",
    TRIM(COALESCE("Phone", '')) AS "Phone",
    INITCAP(TRIM(COALESCE("Title", ''))) AS "Title",
    CASE 
        WHEN UPPER(TRIM(COALESCE("Role__c", ''))) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE("Role__c", ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE("Role__c", ''))) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE("Role__c", ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('ES', 'ESPANOL', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE("Preferred_Language__c", ''))) IN ('IT', 'ITALIANO', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(COALESCE("AccountId", '')) AS "AccountId",
    CAST(NULL AS text) AS "Legacy_Contact_ID__c",
    CAST(NULL AS text) AS "CreatedDate",
    CAST(NULL AS text) AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}