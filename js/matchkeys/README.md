# Matchkeys facilities - JavaScript

## Overview

Matchkeys utilise some specific elements from MARC bibliographic records to generate a unique string which identifies common records that describe the same instance.

The matchkeys in this directory are implemented with JavaScript.

Takes input being a MARC-in-JSON string of MARC fields, and returns the matchkey string.

Each component of the matchkey is padded with the underscore character to fill to its field width.

## Matchkeys implementations

### goldrush

The [js/matchkeys/goldrush](goldrush) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification dated September 2021).

### goldrush2024

The [js/matchkeys/goldrush2024](goldrush2024) implements the "Gold Rush - Colorado Alliance MARC record match key generation" (specification dated 4 December 2024).

## Matchkeys tests of development code

Do 'cd ..' to change to the 'mod-reservoir/js' directory.

Do 'npm install' to install and configure ESLint.

Do 'npm run lint-goldrush2024' and 'npm run lint-test-goldrush2024' etc.

There are some sample MARC files in the [js/test/records](../test/records) directory.

The tests are basic at this stage. Do 'npm run test-goldrush2024' to process the samples.

