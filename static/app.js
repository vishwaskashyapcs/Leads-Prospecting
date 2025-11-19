// DOM Elements
const industryEl = document.getElementById("industry");
const sizeMinEl = document.getElementById("sizeMin");
const sizeMaxEl = document.getElementById("sizeMax");
const rolesEl = document.getElementById("roles");
const searchBtn = document.getElementById("searchBtn");
const resetBtn = document.getElementById("resetBtn");

const countriesToggle = document.getElementById("countriesToggle");
const countriesPanel = document.getElementById("countriesPanel");
const countriesSummary = document.getElementById("countriesSummary");
const countriesAllBtn = document.getElementById("countriesAllBtn");
const countriesClearBtn = document.getElementById("countriesClearBtn");
const countriesDoneBtn = document.getElementById("countriesDoneBtn");

const resultsCard = document.getElementById("results");
const metaEl = document.getElementById("meta");
const downloadEl = document.getElementById("download");
const tableWrap = document.getElementById("tableWrap");

let latestCompanies = [];


// ------------------ Countries Dropdown Logic ------------------
function getCountryCheckboxes() {
  return countriesPanel ? Array.from(countriesPanel.querySelectorAll('input[type="checkbox"]')) : [];
}
function getSelectedCountries() {
  return getCountryCheckboxes().filter(cb => cb.checked).map(cb => cb.value);
}
function setSelectedCountries(values) {
  const set = new Set(values || []);
  getCountryCheckboxes().forEach(cb => (cb.checked = set.has(cb.value)));
  updateCountriesSummary();
}
function updateCountriesSummary() {
  if (!countriesSummary) return;
  const vals = getSelectedCountries();
  countriesSummary.textContent = vals.length ? vals.join(", ") : "Select countries";
}
function openCountriesPanel() { countriesPanel?.classList.add("open"); }
function closeCountriesPanel() { countriesPanel?.classList.remove("open"); }

countriesToggle?.addEventListener("click", (e) => {
  e.stopPropagation();
  countriesPanel.classList.contains("open") ? closeCountriesPanel() : openCountriesPanel();
});
document.addEventListener("click", (e) => {
  if (!countriesPanel?.classList.contains("open")) return;
  if (!countriesPanel.contains(e.target) && !countriesToggle.contains(e.target)) closeCountriesPanel();
});
countriesPanel?.addEventListener("change", updateCountriesSummary);
countriesAllBtn?.addEventListener("click", () => { getCountryCheckboxes().forEach(cb => (cb.checked = true)); updateCountriesSummary(); });
countriesClearBtn?.addEventListener("click", () => { getCountryCheckboxes().forEach(cb => (cb.checked = false)); updateCountriesSummary(); });
countriesDoneBtn?.addEventListener("click", () => { updateCountriesSummary(); closeCountriesPanel(); });
updateCountriesSummary();


// ------------------ Lead Search Logic ------------------
async function runSearch() {
  const payload = {
    industry_focus: industryEl.value,
    company_size_min: parseInt(sizeMinEl.value, 10),
    company_size_max: parseInt(sizeMaxEl.value, 10),
    countries: getSelectedCountries(),
    roles: rolesEl.value.split(",").map(s => s.trim()).filter(Boolean)
  };

  if (!payload.countries.length) {
    alert("Please select at least one country.");
    return;
  }

  resultsCard.style.display = "none";
  metaEl.innerHTML = "";
  downloadEl.innerHTML = "";
  tableWrap.innerHTML = "Searching...";

  try {
    const res = await fetch("/api/leads/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();

    if (!res.ok || !data.ok) throw new Error(data.error || "Lead search failed");

    const items = data.items || [];
    const rejected = typeof data.rejected_count === "number" ? data.rejected_count : 0;
    const metaParts = [`<p><b>Total:</b> ${items.length}</p>`];
    if (rejected) metaParts.push(`<p><b>Rejected:</b> ${rejected}</p>`);

    resultsCard.style.display = "block";
    metaEl.innerHTML = metaParts.join("");
    downloadEl.innerHTML = data.download_url ? `<a href="${data.download_url}">Download JSON</a>` : "";

    buildCompaniesTable(items);
  } catch (err) {
    resultsCard.style.display = "block";
    tableWrap.innerHTML = `<div>Error: ${err.message}</div>`;
  }
}


// ------------------ Build table with Find Lead button ------------------
function buildCompaniesTable(items = []) {
  latestCompanies = Array.isArray(items) ? [...items] : [];

  if (!latestCompanies.length) {
    tableWrap.innerHTML = "<div>No companies found.</div>";
    return;
  }

  let html = `
    <table>
      <thead>
        <tr>
          <th>Company</th>
          <th>Size</th>
          <th>City</th>
          <th>Country</th>
          <th>Website</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
  `;

  latestCompanies.forEach((c, idx) => {
    const companyName = c?.company_name || "N/A";
    const companySize = c?.company_size || "N/A";
    const city = c?.city || "N/A";
    const country = c?.country || "N/A";
    const websiteDisplay = c?.website
      ? `<a href="${c.website}" target="_blank" rel="noopener">${c.website}</a>`
      : "N/A";

    html += `
      <tr id="row-${idx}">
        <td>${companyName}</td>
        <td>${companySize}</td>
        <td>${city}</td>
        <td>${country}</td>
        <td>${websiteDisplay}</td>
        <td class="actions-cell">
          <button class="btn" onclick="enrichCompany(${idx})">Enrich</button>
          <button class="btn primary" onclick="findLead(${idx})">Find Lead</button>
        </td>
      </tr>
      <tr id="enrich-${idx}" style="display:none;">
        <td colspan="6" class="lead-box">Preparing enrichment...</td>
      </tr>
      <tr id="lead-${idx}" style="display:none;">
        <td colspan="6" class="lead-box">Fetching lead...</td>
      </tr>
    `;
  });

  html += "</tbody></table>";
  tableWrap.innerHTML = html;
}


// ------------------ Find Lead flow (SERP + LinkedIn) ------------------
async function findLead(idx) {
  const company = latestCompanies[idx];
  const companyName = company?.company_name || "N/A";
  const box = document.getElementById(`lead-${idx}`);
  if (!company || !box) return;

  box.style.display = "table-row";
  box.innerHTML = `<td colspan="6">Searching LinkedIn for ${companyName}...</td>`;

  try {
    const res = await fetch("/api/company/find-lead", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ company_name: companyName }),
    });

    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || "Lead lookup failed");

    const people = Array.isArray(data.people) ? data.people : [];
    if (!people.length) {
      box.innerHTML = `<td colspan="6">No person found.</td>`;
      return;
    }

    const p = people[0];
    const linkedin = p.linkedin
      ? `<a href="${p.linkedin}" target="_blank" rel="noopener">${p.linkedin}</a>`
      : "LinkedIn unavailable";

    box.innerHTML = `
      <td colspan="6">
        <div><strong>${p.name || "Unknown contact"}</strong></div>
        <div>${p.role || "Role unavailable"}</div>
        <div>${linkedin}</div>
      </td>
    `;

  } catch (err) {
    box.innerHTML = `<td colspan="6">Error: ${err.message}</td>`;
  }
}


async function enrichCompany(idx) {
  const company = latestCompanies[idx];
  const companyName = company?.company_name || "N/A";
  const website = company?.website || "";
  const box = document.getElementById(`enrich-${idx}`);
  if (!company || !box) return;

  box.style.display = "table-row";
  box.innerHTML = `<td colspan="6">Enriching ${companyName}...</td>`;

  try {
    const payload = { company_name: companyName };
    if (website) payload.website = website;

    const res = await fetch("/api/company/enrich", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || "Enrichment failed");

    const info = data.data || {};
    const rows = [
      ["Company", info["Company Name"]],
      ["Website", info["Website URL"]],
      ["Email", info["Email ID"]],
      ["Phone", info["Phone (if verified)"]],
      ["LinkedIn", info["LinkedIn Profile URL"]],
      ["Location", info["Country / City"]],
      ["Industry", info["Industry Segment"] || info["Industry Type (Hotel / Resort / Service Apartment, etc.)"]],
      ["Google Rating", info["Google Rating"]],
      ["Total Reviews", info["Total Google Reviews"]],
    ].filter(([, value]) => value);

    const summary = rows.length
      ? rows.map(([label, value]) => `<div><b>${label}:</b> ${value}</div>`).join("")
      : "<div>No enrichment data returned.</div>";

    box.innerHTML = `<td colspan="6">${summary}</td>`;
  } catch (err) {
    box.innerHTML = `<td colspan="6">Error: ${err.message}</td>`;
  }
}


// ------------------ Reset ------------------
function resetForm() {
  industryEl.value = "Hospitality & Travel";
  sizeMinEl.value = "50";
  sizeMaxEl.value = "5000";
  rolesEl.value = "CEO, COO, Head of Operations, General Manager, GM";
  setSelectedCountries([]);
  resultsCard.style.display = "none";
  metaEl.innerHTML = "";
  downloadEl.innerHTML = "";
  tableWrap.innerHTML = "";
}


// ------------------ Event Listeners ------------------
searchBtn?.addEventListener("click", runSearch);
resetBtn?.addEventListener("click", resetForm);

console.log("Lead finder loaded.");
