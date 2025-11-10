// ==========================
// Legacy quick lookup (/api/run)
// ==========================
const form = document.getElementById("lead-form");
const statusBox = document.getElementById("status");
const statusLine = document.getElementById("status-line");
const resultBox = document.getElementById("result");
const tipEl = document.getElementById("dynamic-tip");

const tips = [
  "Searching Google…",
  "Picking the official website…",
  "Crawling pages for emails, phones, LinkedIn…",
  "Parsing ratings and address from JSON-LD…",
  "Normalizing to your schema…",
  "Saving your JSON file…",
];

let tipIdx = 0;
let tipTimer = null;

function startTips() {
  if (!tipEl) return;
  tipIdx = 0;
  tipEl.textContent = tips[tipIdx];
  tipTimer = setInterval(() => {
    tipIdx = (tipIdx + 1) % tips.length;
    tipEl.textContent = tips[tipIdx];
  }, 1200);
}

function stopTips() {
  if (tipTimer) { clearInterval(tipTimer); tipTimer = null; }
}

if (form) {
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (resultBox) resultBox.classList.add("hidden");
    if (statusBox) statusBox.classList.remove("hidden");
    if (statusLine) statusLine.textContent = "Working… please wait.";
    startTips();

    const queryEl = document.getElementById("query");
    const query = queryEl ? queryEl.value.trim() : "";

    try {
      const resp = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query })
      });
      const data = await resp.json();
      stopTips();

      if (!data.ok) {
        if (statusLine) statusLine.textContent = "Something went wrong.";
        if (resultBox) {
          resultBox.classList.remove("hidden");
          resultBox.innerHTML = `<div>⚠️ ${data.error || "Unknown error"}</div>`;
        }
        return;
      }

      if (statusBox) statusBox.classList.add("hidden");
      if (resultBox) {
        resultBox.classList.remove("hidden");
        resultBox.innerHTML = `
          <div>✅ Done! Download your JSON:
            <a href="${data.json_path}">Download</a>
          </div>
        `;
      }
    } catch (err) {
      stopTips();
      if (statusLine) statusLine.textContent = "Error calling backend.";
      if (resultBox) {
        resultBox.classList.remove("hidden");
        resultBox.innerHTML = `<div>⚠️ ${err.message}</div>`;
      }
    }
  });
}

// ==========================
// Multi-select dropdown (Countries)
// ==========================
const countriesToggle = document.getElementById('countriesToggle');
const countriesPanel  = document.getElementById('countriesPanel');
const countriesSummary = document.getElementById('countriesSummary');
const countriesAllBtn = document.getElementById('countriesAllBtn');
const countriesClearBtn = document.getElementById('countriesClearBtn');
const countriesDoneBtn = document.getElementById('countriesDoneBtn');

function getCountryCheckboxes() {
  return countriesPanel ? Array.from(countriesPanel.querySelectorAll('input[type="checkbox"]')) : [];
}

function getSelectedCountries() {
  return getCountryCheckboxes().filter(cb => cb.checked).map(cb => cb.value);
}

function setSelectedCountries(values) {
  const set = new Set(values || []);
  getCountryCheckboxes().forEach(cb => { cb.checked = set.has(cb.value); });
  updateCountriesSummary();
}

function updateCountriesSummary() {
  if (!countriesSummary) return;
  const vals = getSelectedCountries();
  countriesSummary.textContent = vals.length ? vals.join(', ') : 'Select countries';
}

function openCountriesPanel() {
  if (!countriesPanel) return;
  countriesPanel.classList.add('open');
  countriesPanel.setAttribute('aria-hidden', 'false');
}

function closeCountriesPanel() {
  if (!countriesPanel) return;
  countriesPanel.classList.remove('open');
  countriesPanel.setAttribute('aria-hidden', 'true');
}

// Toggle open/close
if (countriesToggle && countriesPanel) {
  countriesToggle.addEventListener('click', (e) => {
    e.stopPropagation();
    const isOpen = countriesPanel.classList.contains('open');
    if (isOpen) closeCountriesPanel(); else openCountriesPanel();
  });

  // Update summary on any checkbox change
  countriesPanel.addEventListener('change', (e) => {
    if (e.target && e.target.matches('input[type="checkbox"]')) {
      updateCountriesSummary();
    }
  });
}

// Click outside to close
document.addEventListener('click', (e) => {
  if (!countriesPanel || !countriesToggle) return;
  if (!countriesPanel.classList.contains('open')) return;
  const withinPanel = countriesPanel.contains(e.target);
  const withinButton = countriesToggle.contains(e.target);
  if (!withinPanel && !withinButton) closeCountriesPanel();
});

// Buttons inside panel
if (countriesAllBtn) {
  countriesAllBtn.addEventListener('click', () => {
    getCountryCheckboxes().forEach(cb => cb.checked = true);
    updateCountriesSummary();
  });
}
if (countriesClearBtn) {
  countriesClearBtn.addEventListener('click', () => {
    getCountryCheckboxes().forEach(cb => cb.checked = false);
    updateCountriesSummary();
  });
}
if (countriesDoneBtn) {
  countriesDoneBtn.addEventListener('click', () => {
    updateCountriesSummary();   // ensure summary reflects latest
    closeCountriesPanel();
  });
}

// Initial summary text
updateCountriesSummary();

// ==========================
// Filtered lead search (/api/leads/search)
// ==========================
const resultsCard = document.getElementById("results");
const metaEl = document.getElementById("meta");
const downloadEl = document.getElementById("download");
const tableWrap = document.getElementById("tableWrap");

window.runSearch = async function runSearch() {
  const industryEl = document.getElementById('industry');
  const sizeMinEl  = document.getElementById('sizeMin');
  const sizeMaxEl  = document.getElementById('sizeMax');
  const rolesEl    = document.getElementById('roles');

  if (!industryEl || !sizeMinEl || !sizeMaxEl || !rolesEl) {
    alert("Missing one or more filter inputs on the page.");
    return;
  }

  const industry = industryEl.value;
  const sizeMin  = parseInt(sizeMinEl.value, 10);
  const sizeMax  = parseInt(sizeMaxEl.value, 10);
  const countries = getSelectedCountries();
  const roles = rolesEl.value.split(',').map(s => s.trim()).filter(Boolean);

  if (!countries.length) {
    alert('Please select at least one country.');
    return;
  }

  const payload = {
    industry_focus: industry,
    company_size_min: sizeMin,
    company_size_max: sizeMax,
    countries,
    roles
  };

  try {
    const res = await fetch('/api/leads/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const t = await res.text();
      throw new Error(t || 'Request failed');
    }

    const data = await res.json();

    if (resultsCard) resultsCard.style.display = 'block';
    if (metaEl) metaEl.innerHTML = `<p><b>Total:</b> ${data.total} &nbsp; <b>Request ID:</b> ${data.request_id}</p>`;
    if (downloadEl) downloadEl.innerHTML = `<a href="${data.download_url}">Download JSON</a>`;

    const cols = ["company_name","company_size","country","city","website","linkedin_url","role","person_name","person_email","person_linkedin"];
    let html = '<table><thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead><tbody>';
    for (const row of data.items) {
      html += '<tr>' + cols.map(c => `<td>${row[c] ?? ""}</td>`).join('') + '</tr>';
    }
    html += '</tbody></table>';

    if (tableWrap) tableWrap.innerHTML = html;

  } catch (err) {
    if (resultsCard) resultsCard.style.display = 'block';
    if (metaEl) metaEl.innerHTML = '';
    if (downloadEl) downloadEl.innerHTML = '';
    if (tableWrap) tableWrap.innerHTML = `<div>⚠️ ${err.message}</div>`;
  }
};

window.resetForm = function resetForm() {
  const industryEl = document.getElementById('industry');
  const sizeMinEl  = document.getElementById('sizeMin');
  const sizeMaxEl  = document.getElementById('sizeMax');
  const rolesEl    = document.getElementById('roles');

  if (industryEl) industryEl.value = 'Hospitality & Travel';
  if (sizeMinEl)  sizeMinEl.value  = '50';
  if (sizeMaxEl)  sizeMaxEl.value  = '5000';
  if (rolesEl)    rolesEl.value    = 'CEO, COO, Head of Operations, General Manager, GM';

  // default: nothing selected
  setSelectedCountries([]);

  if (resultsCard) resultsCard.style.display = 'none';
  if (metaEl) metaEl.innerHTML = '';
  if (downloadEl) downloadEl.innerHTML = '';
  if (tableWrap) tableWrap.innerHTML = '';
};
