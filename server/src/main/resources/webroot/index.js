const form = document.forms.namedItem("uploadForm");
form.addEventListener(
  "submit",
  (event) => {
    //disable default form handling
    event.preventDefault();
    const output = document.querySelector("#output");
    output.innerHTML = "Uploading...";
    //only file input sent as form-data, query params for rest
    const tenantOrToken = document.querySelector("#tokenInput").value;
    const params = {};
    params.sourceId = document.querySelector("#sourceIdInput").value;
    params.sourceVersion = document.querySelector("#sourceVersionInput").value;
    params.localIdPath = document.querySelector("#localIdPathInput").value;
    params.xmlFixing = document.querySelector("#xmlFixingInput").checked;
    const fileInput = document.querySelector("#fileInput");
    const formData = new FormData();
    Array.from(fileInput.files).forEach(file => {
      formData.append("records", file);
    });
    const request = new XMLHttpRequest();
    request.open("POST", "/reservoir/upload" + qs(params), true);
    if (isJWT(tenantOrToken)) {
      request.setRequestHeader("X-Okapi-Token", tenantOrToken);
    } else {
      //local
      request.setRequestHeader("X-Okapi-Tenant", tenantOrToken);
      request.setRequestHeader("X-Okapi-Permissions", '["reservoir-upload.source.' + params.sourceId + '"]');
    }
    request.onload = (progress) => {
      output.innerHTML =
        request.status === 200
          ? renderStatus(request.response)
          : `Error: ${request.responseText}`;
    };
    request.send(formData);
  },
  false
);

function qs(params) {
  let qs = "";
  let sep = "?";
  for (const key in params) {
    if (params[key] === null) continue;
    if (typeof params[key] === "string" && params[key].length === 0) continue;
    qs += sep + key + "=" + encodeURIComponent(params[key]);
    sep = "&";
  }
  return qs;
}

function isJWT(token) {
  return token.startsWith("eyJ");
}

function renderStatus(response) {
  const status = JSON.parse(response);
  const keys = Object.keys(status);
  let summary = "Uploaded. Summary:";
  keys.forEach(key => {
    summary += "<br />File '" + key + "':"
    + "<br/>- processed: " + status[key].processed
    + "<br/>- ignored: " + status[key].ignored
    + "<br/>- inserted: " + status[key].inserted
    + "<br/>- updated: " + status[key].updated
    + "<br/>- ignored: " + status[key].deleted
  });
  return summary;
}
