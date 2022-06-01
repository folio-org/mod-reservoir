# JavaScript facilities

## matchkeys

Match keys utilise some specific elements from MARC bibliographic records to generate a unique string which identifies common records that describe the same instance.

### goldrush

The [matchkeys/goldrush](matchkeys/goldrush/goldrush.js) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification September 2021).

It takes input being a MARC-in-JSON string of MARC fields, and returns the matchkey string.

### matchkeys tests

Do 'yarn install' in this top-level directory to install and configure ESLint. Do 'yarn lint-goldrush' and 'yarn lint-test'.

There are some sample MARC files in the [test/records](test/records) directory.

The tests are basic at this stage. Do 'yarn test' to process the samples.
