// Generate GoldRush match key.
// Reads MARC JSON from input string.
// Extracts and normalizes data from relevant fields.
// Returns matchkey as string.
//
// Version: 1.0.0 (for specification September 2021)

function loadMarcJson(marcJson) {
  const marcObj = JSON.parse(marcJson);
  if (marcObj.fields === undefined) {
    throw new Error('MARC fields array is missing.');
  }
  if (!Array.isArray(marcObj.fields)) {
    throw new Error('MARC fields is not an array.');
  }
  const field0 = Object.keys(marcObj.fields[0]);
  const re = /^\d+$/;
  if (!re.test(field0)) {
    throw new Error('MARC fields[0] key is not numeric.');
  }
  if (!marcObj.leader) {
    throw new Error('MARC leader field is missing.');
  }
  return marcObj;
}

function hasField(record, tag) {
  let result = false;
  result = record.fields.some((f) => f[tag]);
  return result;
}

function getField(record, tag, sf) {
  // Get the first relevant field or subfield.
  let data = null;
  const fields = record.fields.filter((f) => f[tag]);
  // Use the first relevant field
  const f = fields[0];
  if (f !== undefined) {
    if (f[tag].subfields) {
      for (let n = 0; n < f[tag].subfields.length; n += 1) {
        const s = f[tag].subfields[n];
        if (s[sf]) {
          data = s[sf];
          // Use the first relevant subfield
          break;
        }
      }
    } else {
      data = f[tag];
    }
  }
  return data;
}

function stripPunctuation(keyPart, replaceChar) {
  let trimmed = keyPart;
  trimmed = trimmed.replace(/%22/g, '_');
  trimmed = trimmed.replace(/%/g, '_');
  trimmed = trimmed.replace(/^ *[aA] +/, '');
  trimmed = trimmed.replace(/^ *[aA]n +/, '');
  trimmed = trimmed.replace(/^ *[tT]he +/, '');
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
  // FIXME: Handle the note of the spec 245$6
  let fieldStr = '';
  for (let n = 0; n < fieldData.length; n += 1) {
    if (fieldData[n] !== null) {
      fieldStr += stripPunctuation(fieldData[n], ' ');
    }
  }
  fieldStr = fieldStr.trim().normalize('NFD');
  return padContent(fieldStr, 70);
}

function doGMD(fieldData) {
  // General medium designator
  let fieldStr = '';
  if (fieldData !== null) {
    fieldStr = fieldData.replace(/[^a-zA-Z0-9]/g, '');
    fieldStr = fieldStr.replace(/[À-ž]/g, '');
  }
  return padContent(fieldStr, 5);
}

function doPublicationYear(fieldData) {
  let fieldStr = '';
  for (let n = 0; n < fieldData.length; n += 1) {
    if (fieldData[n] !== null) {
      let dataStr = '';
      if (n === 0) {
        // Try for date2 from field 008
        dataStr = `${fieldData[n]}`.substring(11, 15).replace(/[^0-9]/g, '');
        if ((dataStr.match(/[0-9]{4}/)) && (dataStr !== '9999')) {
          fieldStr = dataStr;
          break;
        }
      } else if (n === 1) {
        // Try for date from field 264$c
        dataStr = `${fieldData[n]}`.replace(/[^0-9]/g, '');
        if ((dataStr.match(/[0-9]{4}/)) && (dataStr !== '9999')) {
          fieldStr = dataStr;
          break;
        }
      } else {
        // Try for date from field 260$c
        dataStr = `${fieldData[n]}`.replace(/[^0-9]/g, '');
        if ((dataStr.match(/[0-9]{4}/)) && (dataStr !== '9999')) {
          fieldStr = dataStr;
          break;
        }
      }
    }
  }
  if (!fieldStr) {
    fieldStr = '0000';
  }
  return padContent(fieldStr, 4);
}

function doPagination(fieldData) {
  let fieldStr = '';
  if (fieldData !== null) {
    fieldStr = fieldData.replace(/[^0-9]/g, '');
  }
  return padContent(fieldStr, 4);
}

function doPublisherName(fieldData) {
  let fieldStr = '';
  for (let n = 0; n < fieldData.length; n += 1) {
    if (fieldData[n] !== null) {
      if (n === 0) {
        // Try first for field 264$a
        fieldStr = `${fieldData[n]}`.toLowerCase();
        break;
      } else {
        // Try then for field 260$a
        fieldStr = `${fieldData[n]}`.toLowerCase();
      }
    }
  }
  fieldStr = stripPunctuation(fieldStr, '_');
  return padContent(fieldStr, 5);
}

function doTypeOfRecord(fieldData) {
  let fieldStr = '';
  if (fieldData.length > 10) {
    fieldStr = fieldData.substring(6, 7);
  }
  return fieldStr;
}

function doTitleNumber(fieldData) {
  let fieldStr = '';
  if (fieldData !== null) {
    fieldStr = stripPunctuation(fieldData, '_');
  }
  return padContent(fieldStr, 10);
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

function doInclusiveDates(fieldData) {
  let fieldStr = '';
  if (fieldData !== null) {
    fieldStr = stripPunctuation(fieldData.replace(/ /g, ''), '_');
  }
  return padContent(fieldStr, 15);
}

function doGDCN(fieldData) {
  // Government Document Classification Number
  let fieldStr = '';
  if (fieldData !== null) {
    fieldStr = stripPunctuation(fieldData, '_');
    // Remove accented characters
    fieldStr = fieldStr.replace(/[À-ž]/g, '');
    // Limit maximum field length
    fieldStr = fieldStr.substring(0, 32000);
  }
  return fieldStr;
}

function doElectronicIndicator(marcObj) {
  let field = '';
  field = getField(marcObj, '245', 'h');
  if (field) {
    if (field.match(/\belectronic resource\b/i)) {
      return 'e';
    }
  }
  field = getField(marcObj, '590', 'a');
  if (field) {
    if (field.match(/\belectronic reproduction\b/i)) {
      return 'e';
    }
  }
  field = getField(marcObj, '533', 'a');
  if (field) {
    if (field.match(/\belectronic reproduction\b/i)) {
      return 'e';
    }
  }
  field = getField(marcObj, '300', 'a');
  if (field) {
    if (field.match(/\bonline resource\b/i)) {
      return 'e';
    }
  }
  field = getField(marcObj, '007');
  if (field) {
    if (field.substring(0, 1) === 'c') {
      return 'e';
    }
  }
  // RDA
  field = getField(marcObj, '337', 'a');
  if (field) {
    if (field.substring(0, 1) === 'c') {
      return 'e';
    }
  }
  // other electronic document
  if (hasField(marcObj, '086') || hasField(marcObj, '856')) {
    return 'e';
  }
  return 'p';
}

function addComponent(component) {
  // Assist debug
  const debug = false;
  let delimiter = '';
  if (debug) {
    delimiter = '|';
  }
  return `${delimiter}${component}`;
}

function matchkey(marcJson) {
  let keyStr = '';
  const marcObj = loadMarcJson(marcJson);
  keyStr += addComponent(doTitle([
    getField(marcObj, '245', 'a'),
    getField(marcObj, '245', 'b'),
    getField(marcObj, '245', 'p'),
  ]));
  keyStr += addComponent(doGMD(getField(marcObj, '245', 'h')));
  keyStr += addComponent(doPublicationYear([
    getField(marcObj, '008'),
    getField(marcObj, '264', 'c'),
    getField(marcObj, '260', 'c'),
  ]));
  keyStr += addComponent(doPagination(getField(marcObj, '300', 'a')));
  keyStr += addComponent(doPublisherName([
    getField(marcObj, '264', 'b'),
    getField(marcObj, '260', 'b'),
  ]));
  keyStr += addComponent(doTypeOfRecord(marcObj.leader));
  keyStr += addComponent(doTitleNumber(getField(marcObj, '245', 'n')));
  keyStr += addComponent(doAuthor([
    getField(marcObj, '100', 'a'),
    getField(marcObj, '110', 'a'),
    getField(marcObj, '111', 'a'),
    getField(marcObj, '113', 'a'),
  ]));
  keyStr += addComponent(doInclusiveDates(getField(marcObj, '245', 'f')));
  keyStr += addComponent(doGDCN(getField(marcObj, '086', 'a')));
  keyStr += addComponent(doElectronicIndicator(marcObj));
  return keyStr.toLowerCase();
}

module.exports = { matchkey };
