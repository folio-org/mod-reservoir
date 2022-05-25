// Generate GoldRush match key.
// Reads MARC JSON from input string.
// Extracts and normalizes data from relevant fields.
// Returns matchkey as string.

function loadMarcJson(marcJson) {
  let marcObj;
  try {
    marcObj = JSON.parse(marcJson);
  } catch (e) {
    throw new Error(e.message);
  }
  if (marcObj.fields === undefined) {
    throw new Error('MARC "fields" array is missing.');
  }
  if (!Array.isArray(marcObj.fields)) {
    throw new Error('MARC "fields" is not an array.');
  }
  const field0 = Object.keys(marcObj.fields[0]);
  const re = /^\d+$/;
  if (!re.test(field0)) {
    throw new Error('MARC "fields[0]" key is not numeric.');
  }
  return marcObj;
}

function getField(record, tag, sf) {
  let data = null;
  const fields = record.fields.filter((f) => f[tag]);
  fields.forEach((f) => {
    if (f[tag].subfields) {
      f[tag].subfields.forEach((s) => {
        if (s[sf]) {
          data = s[sf];
        }
      });
    } else {
      data = f[tag];
    }
  });
  return data;
}

function stripPunctuation(keyPart, replaceChar) {
  // FIXME: add the many other cases defined in spec
  let trimmed = keyPart;
  trimmed = trimmed.replace(/[,.]/g, replaceChar);
  trimmed = trimmed.replace(/['}{:]/g, '');
  return trimmed;
}

function padContent(keyPart, length) {
  let padded = keyPart;
  padded = padded.replace(/ +/g, ' ');
  padded = padded.replace(/ /g, '_');
  padded = padded.substring(0, length).padEnd(length, '_');
  return padded;
}

function doTitle(fieldData) {
  // FIXME: handle the other subflieds
  // FIXME: Handle the note of the spec
  const fieldStr = stripPunctuation(fieldData, ' ').trim();
  // FIXME: Do normalize
  return padContent(fieldStr, 70);
}

function doAuthor(fieldData) {
  let fieldStr = '';
  for (let n = 0; n < fieldData.length; n += 1) {
    if (fieldData[n] !== null) {
      let dataStr = stripPunctuation(fieldData[n], '_');
      // Remove accented characters
      dataStr = dataStr.replace(/[À-ž]/g, '');
      fieldStr += dataStr;
    }
  }
  return padContent(fieldStr, 20);
}

function matchkey(marcJson) {
  let keyStr = '';
  const marcObj = loadMarcJson(marcJson);
  keyStr += doTitle(getField(marcObj, '245', 'a'));
  keyStr += doAuthor([
    getField(marcObj, '100', 'a'),
    getField(marcObj, '110', 'a'),
    getField(marcObj, '111', 'a'),
    getField(marcObj, '113', 'a'),
  ]);
  return keyStr.replace(/^_/, '');
}

module.exports = { matchkey };
