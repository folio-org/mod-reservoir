# Matchkeys facilities - JavaScript

## Overview

Matchkeys utilise some specific elements from MARC bibliographic records to generate a unique string which identifies common records that describe the same instance.

The matchkeys in this directory are implemented with JavaScript.

Takes input being a MARC-in-JSON string of MARC fields, and returns the matchkey string.

Each component of the matchkey is padded with the underscore character to fill to its field width.

## Matchkeys implementations

### goldrush

The [js/matchkeys/goldrush](goldrush) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification September 2021).

### goldrush2024

Note: This is an experiment, only present on the branch [RSRVR-132-goldrush](https://github.com/folio-org/mod-reservoir/tree/RSRVR-132-goldrush).

The [js/matchkeys/goldrush](goldrush) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification January 12, 2024).

## Matchkeys tests of development code

Do 'cd ..' to change to the 'mod-reservoir/js' directory.

Do 'npm install' to install and configure ESLint.

Do 'npm run lint-goldrush' and 'npm run lint-test'.

There are some sample MARC files in the [js/test/records](../test/records) directory.

The tests are basic at this stage. Do 'npm run test' to process the samples.

