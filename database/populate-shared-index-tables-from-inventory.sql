-- MATCH_VALUEs

INSERT INTO diku_mod_reshare_index.match_value (match_type, value)
SELECT 'MATCH KEY'::diku_mod_reshare_index.match_type, jsonb->>'matchKey'
FROM diku_mod_inventory_storage.instance 
UNION
SELECT DISTINCT match_type, isbn FROM
  (SELECT 'ISBN'::diku_mod_reshare_index.match_type as match_type, 
       (SELECT identifier->>'value' FROM
         (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
          WHERE identifier->>'identifierTypeId' = '8261054f-be78-422d-bd51-4ed9f33c3422'
          LIMIT 1) AS isbn
   FROM diku_mod_inventory_storage.instance) as values
WHERE values.isbn IS NOT NULL
UNION
SELECT DISTINCT match_type, issn FROM
  (SELECT 'ISSN'::diku_mod_reshare_index.match_type as match_type, 
       (SELECT identifier->>'value' FROM
         (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
          WHERE identifier->>'identifierTypeId' = '913300b2-03ed-469a-8179-c1092c991227'
          LIMIT 1) AS issn
   FROM diku_mod_inventory_storage.instance) as values
WHERE values.issn IS NOT NULL
UNION 
SELECT  DISTINCT match_type, publisher_distributor_number FROM
  (SELECT 'PUBLISHER/DISTRIBUTOR NUMBER'::diku_mod_reshare_index.match_type as match_type, 
       (SELECT identifier->>'value' FROM
         (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
          WHERE identifier->>'identifierTypeId' = 'b5d8cdc4-9441-487c-90cf-0c7ec97728eb'
          LIMIT 1) AS publisher_distributor_number
   FROM diku_mod_inventory_storage.instance) as values
WHERE values.publisher_distributor_number IS NOT NULL
ON CONFLICT DO NOTHING
;

-- BIB_RECORDs

INSERT INTO diku_mod_reshare_index.bib_record
(local_identifier, library_id, title, match_key, isbn, issn, publisher_distributor_number, source)
SELECT (SELECT identifier->>'value' FROM
        (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
         WHERE identifier->>'identifierTypeId' = :identifier_type_id
         LIMIT 1) AS local_identifier, :identifier_type_id as library_id, 
       jsonb->>'title', 
       jsonb->>'matchKey',        
       (SELECT identifier->>'value' FROM
        (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
         WHERE identifier->>'identifierTypeId' = '8261054f-be78-422d-bd51-4ed9f33c3422'
         LIMIT 1) AS isbn,        
       (SELECT identifier->>'value' FROM
        (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
         WHERE identifier->>'identifierTypeId' = '913300b2-03ed-469a-8179-c1092c991227'
         LIMIT 1) AS issn,        
       (SELECT identifier->>'value' FROM
        (SELECT jsonb_array_elements((jsonb->>'identifiers')::JSONB) as identifier) AS identifiers 
         WHERE identifier->>'identifierTypeId' = 'b5d8cdc4-9441-487c-90cf-0c7ec97728eb'
         LIMIT 1) AS publisher_distributor_number,        
       jsonb
 FROM diku_mod_inventory_storage.instance
;

-- MATCHes

INSERT INTO diku_mod_reshare_index.match (match_value_id, bib_id) 
SELECT match_value.id match_value_id, bib_record.id bib_record_id
FROM  diku_mod_reshare_index.match_value,
      diku_mod_reshare_index.bib_record
WHERE bib_record.match_key = match_value.value
UNION
SELECT match_value.id match_value_id, bib_record.id bib_record_id
FROM  diku_mod_reshare_index.match_value,
      diku_mod_reshare_index.bib_record
WHERE bib_record.isbn = match_value.value
UNION
SELECT match_value.id match_value_id, bib_record.id bib_record_id
FROM  diku_mod_reshare_index.match_value,
      diku_mod_reshare_index.bib_record
WHERE bib_record.issn = match_value.value
UNION
SELECT match_value.id match_value_id, bib_record.id bib_record_id
FROM  diku_mod_reshare_index.match_value,
      diku_mod_reshare_index.bib_record
WHERE bib_record.publisher_distributor_number = match_value.value
; 

-- ITEMs

INSERT INTO diku_mod_reshare_index.item (bib_id, barcode, shareable)
SELECT bib_record.id bib_id, item.jsonb->>'barcode' barcode, TRUE
FROM   diku_mod_inventory_storage.item,
       diku_mod_inventory_storage.instance,
       diku_mod_inventory_storage.holdings_record,
       diku_mod_reshare_index.bib_record
WHERE item.holdingsRecordId = holdings_record.id
  AND holdings_record.instanceId = instance.id
  AND instance.jsonb->>'matchKey' = bib_record.match_key
;


