# JavaScript facilities

## Matchkeys

Matchkeys utilise some specific elements from MARC bibliographic records to generate a unique string which identifies common records that describe the same instance.

The various matchkeys implementations are explained at [js/matchkeys](matchkeys).

## Transformers

Example transformer that collects MARC fields from all member records and creates field 999_10 for each with: sourceId, localId and globalId.
