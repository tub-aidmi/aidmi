{{ config(materialized='table') }}

-- Contact model: transforms fixture_missing_relations_v2_src.contact with join to account
WITH normalized_accounts AS (
    SELECT 
        REGEXP_REPLACE(id, '[^A-Z0-9]', '', 'g') AS "normalized_id",
        id AS "raw_id"
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),
contact_staging AS (
    SELECT
        -- Id: normalized contact natural key for cross-model consistency
        TRIM(id) AS "Id",
        
        -- FirstName: split full_name on first space, fallback to full name if single word
        CASE 
            WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN NULL
            ELSE COALESCE(
                NULLIF(TRIM(INITCAP(SPLIT_PART(full_name, ' ', 1))), ''),
                INITCAP(TRIM(full_name))
            )
        END AS "FirstName",
        
        -- LastName: split full_name on second part; NOT NULL constraint → fallback 'Unknown'
        CASE 
            WHEN TRIM(full_name) IS NULL OR TRIM(full_name) = '' THEN 'Unknown'
            ELSE COALESCE(
                NULLIF(TRIM(INITCAP(SPLIT_PART(full_name, ' ', 2))), ''),
                'Unknown'
            )
        END AS "LastName",
        
        -- Email: lowercase and trimmed for standardization
        LOWER(TRIM(email)) AS "Email",
        
        -- Phone: no source equivalent → NULL
        NULL AS "Phone",
        
        -- Title: no source equivalent → NULL  
        NULL AS "Title",
        
        -- Role__c enum: no source column maps to this; default NULL per fallback policy
        NULL::TEXT AS "Role__c",
        
        -- Preferred_Language__c enum: default 'EN' for safety as per plan
        'EN'::TEXT AS "Preferred_Language__c",
        
        -- AccountId: normalized Salesforce-style Account Id from join, or NULL if no match
        na."normalized_id" AS "AccountId",
        
        -- Legacy_Contact_ID__c: raw source natural key for row-level traceability
        TRIM(id) AS "Legacy_Contact_ID__c",
        
        -- CreatedDate / LastModifiedDate: use current date as no source date available
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        
        -- IsDeleted: 0 (active records)
        0::INTEGER AS "IsDeleted"
    
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
    LEFT JOIN normalized_accounts na 
        ON TRIM(UPPER(REGEXP_REPLACE(c.account_ref, '[^A-Z0-9]', '', 'g'))) = TRIM(UPPER(na."normalized_id"))
)

SELECT
    "Id",
    "FirstName",
    "LastName",
    "Email",
    "Phone",
    "Title",
    "Role__c",
    "Preferred_Language__c",
    "AccountId",
    "Legacy_Contact_ID__c",
    "CreatedDate",
    "LastModifiedDate",
    "IsDeleted"

FROM contact_staging