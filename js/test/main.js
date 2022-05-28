const fs = require('fs');
const path = require('path');
const goldrush = require('../matchkeys/goldrush/goldrush');

const testsPath = 'test/records';
const files = fs.readdirSync(testsPath);
const testFiles = files.filter((file) => path.extname(file) === '.json');
for (let n = 0; n < testFiles.length; n += 1) {
  const testFile = `${testsPath}/${testFiles[n]}`;
  const marcJson = fs.readFileSync(testFile, 'utf8');
  console.log(`Processing ${testFile}`);
  let keyStrGoldrush = '';
  try {
    keyStrGoldrush = goldrush.matchkey(marcJson);
  } catch (e) {
    console.log(`Input must be parseable MARC-in-JSON. ${e}`);
  }
  console.log(`${keyStrGoldrush}`);
}
