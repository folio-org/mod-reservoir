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
  let trimmed = keyPart;
  trimmed = trimmed.replace(/%22/g, '_');
  trimmed = trimmed.replace(/%/g, '_');
  trimmed = trimmed.replace(/^ +[aA] +/, '');
  trimmed = trimmed.replace(/^ +[aA]n +/, '');
  trimmed = trimmed.replace(/^ +[tT]he +/, '');
  trimmed = trimmed.replace(/['{}]/g, '');
  trimmed = trimmed.replace(/&/g, 'and');
  trimmed = trimmed.replace(/\u0020/g, replaceChar);
  trimmed = trimmed.replace(/\u0021/g, replaceChar);
  trimmed = trimmed.replace(/\u0022/g, replaceChar);
  trimmed = trimmed.replace(/\u0023/g, replaceChar);
  trimmed = trimmed.replace(/\u0024/g, replaceChar);
  trimmed = trimmed.replace(/\u0028/g, replaceChar);
  trimmed = trimmed.replace(/\u0029/g, replaceChar);
  trimmed = trimmed.replace(/\u002A/g, replaceChar);
  trimmed = trimmed.replace(/\u002B/g, replaceChar);
  trimmed = trimmed.replace(/\u002C/g, replaceChar);
  trimmed = trimmed.replace(/\u002D/g, replaceChar);
  trimmed = trimmed.replace(/\u002E/g, replaceChar);
  trimmed = trimmed.replace(/\u002F/g, replaceChar);
  trimmed = trimmed.replace(/\u003A/g, replaceChar);
  trimmed = trimmed.replace(/\u003B/g, replaceChar);
  trimmed = trimmed.replace(/\u003C/g, replaceChar);
  trimmed = trimmed.replace(/\u003D/g, replaceChar);
  trimmed = trimmed.replace(/\u003E/g, replaceChar);
  trimmed = trimmed.replace(/\u003F/g, replaceChar);
  trimmed = trimmed.replace(/\u0040/g, replaceChar);
  trimmed = trimmed.replace(/\u005B/g, replaceChar);
  trimmed = trimmed.replace(/\\/g, replaceChar);
  trimmed = trimmed.replace(/\u005D/g, replaceChar);
  trimmed = trimmed.replace(/\u005E/g, replaceChar);
  trimmed = trimmed.replace(/\u005F/g, replaceChar);
  trimmed = trimmed.replace(/\u0060/g, replaceChar);
  trimmed = trimmed.replace(/\u007C/g, replaceChar);
  trimmed = trimmed.replace(/\u007E/g, replaceChar);
  trimmed = trimmed.replace(/\u00A9/g, replaceChar);
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
