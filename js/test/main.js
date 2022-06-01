const fs = require('fs');
const path = require('path');
const goldrush = require('../matchkeys/goldrush/goldrush');

function assert(result, message) {
  if (result) {
    console.log('Passed assertion');
  } else {
    console.log(`Failed assertion: Should match:\n${message}`);
  }
  return result;
}

const assertionsFile = 'test/assertions-goldrush.json';
const assertionsJson = fs.readFileSync(assertionsFile, 'utf8');
const assertionsGoldrush = JSON.parse(assertionsJson);
let testsNum = 0;
let testsFailedNum = 0;
const testsPath = 'test/records';
const files = fs.readdirSync(testsPath);
const testFiles = files.filter((file) => path.extname(file) === '.json');
for (let n = 0; n < testFiles.length; n += 1) {
  const testFile = `${testsPath}/${testFiles[n]}`;
  const marcJson = fs.readFileSync(testFile, 'utf8');
  testsNum += 1;
  console.log(`\nProcessing ${testFile}`);
  let keyStrGoldrush = '';
  try {
    keyStrGoldrush = goldrush.matchkey(marcJson);
  } catch (e) {
    keyStrGoldrush = e.message;
  }
  console.log(`${keyStrGoldrush}`);
  const assertion = assertionsGoldrush[testFile];
  if (!assert(keyStrGoldrush === assertion, assertion)) {
    testsFailedNum += 1;
  }
}
console.log(`\nProcessed ${testsNum} test files, failed ${testsFailedNum}`);
