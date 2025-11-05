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

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  resultBox.classList.add("hidden");
  statusBox.classList.remove("hidden");
  statusLine.textContent = "Working… please wait.";
  startTips();

  const query = document.getElementById("query").value.trim();
  try {
    const resp = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query })
    });
    const data = await resp.json();
    stopTips();

    if (!data.ok) {
      statusLine.textContent = "Something went wrong.";
      resultBox.classList.remove("hidden");
      resultBox.innerHTML = `<div>⚠️ ${data.error || "Unknown error"}</div>`;
      return;
    }

    statusBox.classList.add("hidden");
    resultBox.classList.remove("hidden");
    resultBox.innerHTML = `
  <div>✅ Done! Download your JSON:
  <a href="${data.json_path}">Download</a></div>
`;

  } catch (err) {
    stopTips();
    statusLine.textContent = "Error calling backend.";
    resultBox.classList.remove("hidden");
    resultBox.innerHTML = `<div>⚠️ ${err.message}</div>`;
  }
});
