-- create role diku_mod_reshare_index password 'diku' NOSUPERUSER NOCREATEDB INHERIT LOGIN;
-- grant diku_mod_reshare_index to current_user;
-- create schema diku_mod_reshare_index authorization diku_mod_reshare_index;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS diku_mod_reshare_index.BIB_RECORD
(id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
 local_identifier VARCHAR NOT NULL,
 library_id uuid NOT NULL,
 title VARCHAR,
 match_key VARCHAR NOT NULL,
 isbn VARCHAR,  -- (or, if multiple, isbns jsonb)
 issn VARCHAR,
 publisher_distributor_number VARCHAR,
 source JSONB NOT NULL,
 inventory JSONB
);

CREATE UNIQUE INDEX idx_local_id ON 
diku_mod_reshare_index.BIB_RECORD (local_identifier, library_id);

CREATE INDEX idx_bib_record_match_key ON
diku_mod_reshare_index.BIB_RECORD (match_key);

CREATE VIEW diku_mod_reshare_index.ITEM_VIEW AS
SELECT id, local_identifier, library_id, match_key,
       jsonb_array_elements((jsonb_array_elements((inventory->>'holdingsRecords')::JSONB)->>'items')::JSONB) item
FROM diku_mod_reshare_index.bib_record ;




-- Finding items of merged instance
-- With a bib record uuid:

SELECT match_key, local_identifier, library_id, item
FROM  diku_mod_reshare_index.item_view
WHERE item_view.match_key =
     (SELECT match_key
      FROM  diku_mod_reshare_index.bib_record
      WHERE id = '3ac6fdea-103a-49c2-943a-cfa37d77f898' LIMIT 1);

--  With a match key

SELECT match_key, local_identifier, library_id, item
FROM  diku_mod_reshare_index.item_view
WHERE item_view.match_key = 'guesses_at_truth_by_two_brothers_:_first_series';  -- (not a GoldRush compliant match-key)


--                       (PALCI: 36 million records)



-- CREATE TYPE diku_mod_reshare_index.MATCH_TYPE
-- AS ENUM ('MATCH KEY', 'ISBN', 'ISSN', 'PUBLISHER/DISTRIBUTOR NUMBER');


-- CREATE TABLE IF NOT EXISTS diku_mod_reshare_index.MATCH_VALUE
-- (id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
--  match_type diku_mod_reshare_index.MATCH_TYPE NOT NULL,
--  value VARCHAR NOT NULL
-- );

-- CREATE UNIQUE INDEX idx_match_value ON
-- diku_mod_reshare_index.MATCH_VALUE (value, match_type);

--                       (PALCI: 70 million records)

-- ***

-- CREATE TABLE IF NOT EXISTS diku_mod_reshare_index.ITEM
-- (id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
--  bib_id uuid NOT NULL,
--  barcode VARCHAR,
--  shareable BOOLEAN,
--  CONSTRAINT fk_item_bib_record_id FOREIGN KEY (bib_id) REFERENCES diku_mod_reshare_index.BIB_RECORD(id)
-- );

-- CREATE UNIQUE INDEX idx_local_barcode ON
-- diku_mod_reshare_index.ITEM (barcode, bib_id);

--                       (PALCI: 44 million records)
  
                        
-- CREATE TABLE IF NOT EXISTS diku_mod_reshare_index.MATCH
-- (id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
--  match_value_id uuid NOT NULL,
--  bib_id uuid NOT NULL,
--  CONSTRAINT fk_match_value_id FOREIGN KEY (match_value_id) REFERENCES diku_mod_reshare_index.MATCH_VALUE(id),
--  CONSTRAINT fk_matched_recode_bib_id FOREIGN KEY (bib_id) REFERENCES diku_mod_reshare_index.BIB_RECORD(id)
-- );

-- CREATE UNIQUE INDEX idx_record_match ON
-- diku_mod_reshare_index.MATCH (bib_id, match_value_id);

--                       (PALCI: 110 million records:  40 mio records with matchkeys, 30 mio with ISBN, 10 mio with second ISBN, 10 mio with ISSN, 20 mio with publisher number   ~  3 match-entries per bib)   

 
 
