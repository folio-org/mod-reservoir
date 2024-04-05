# JavaScript facilities

## matchkeys

Match keys utilise some specific elements from MARC bibliographic records to generate a unique string which identifies common records that describe the same instance.

### goldrush

The [matchkeys/goldrush](matchkeys/goldrush/goldrush.mjs) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification January 12, 2024).

It takes input being a MARC-in-JSON string of MARC fields, and returns the matchkey string.

### matchkeys tests

Do 'npm install' in this top-level directory to install and configure ESLint. Do 'npm run lint-goldrush' and 'npm run lint-test'.

There are some sample MARC files in the [test/records](test/records) directory.

The tests are basic at this stage. Do 'npm run test' to process the samples.

### transformers

Example transformer that collects MARC fields from all member records and creates field 999_10 for each withe sourceId, localId and globalId.
