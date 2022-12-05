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
    const fileInput = document.querySelector("#fileInput");
    const formData = new FormData();
    formData.append("records", fileInput.files[0]);
    const request = new XMLHttpRequest();
    request.open("POST", "/reservoir/upload" + qs(params), true);
    if (isJWT(tenantOrToken)) {
      request.setRequestHeader("X-Okapi-Token", tenantOrToken);
    } else {
      request.setRequestHeader("X-Okapi-Tenant", tenantOrToken);
    }
    request.onload = (progress) => {
      output.innerHTML =
        request.status === 200
          ? "Uploaded."
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
    if (params[key]) {
      qs += sep + key + "=" + encodeURIComponent(params[key]);
      sep = "&";
    }
  }
  return qs;
}

function isJWT(token) {
  return token.startsWith("eyJ");
}
