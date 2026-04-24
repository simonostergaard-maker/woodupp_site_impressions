"""Patch index.html to add Brand Analysis tab, reorder tabs, and inject brand_analysis data."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).parent

with open(ROOT / "index.html", "r", encoding="utf-8") as f:
    html = f.read()

# ─── 1. Tab order ───
old_tabs = (
    '<div class="tabs" id="tabsBar">\n'
    '  <div class="tab active" data-tab="overview">Dashboard</div>\n'
    '  <div class="tab" data-tab="anonymized">Anonymized Queries</div>\n'
    '  <div class="tab" data-tab="urls">URL Performance</div>\n'
    '  <div class="tab" data-tab="keywords">Keyword Performance</div>\n'
    '  <div class="tab" data-tab="countries">Country Overview</div>\n'
    '  <div class="tab" data-tab="domains">Domain Comparison</div>\n'
    '  <div class="tab" data-tab="devices">Device & Search Type</div>\n'
    '  <div class="tab" data-tab="analytics">Analytics</div>\n'
    '  <div class="tab" data-tab="comparison">Date Comparison</div>\n'
    '  <div class="tab" data-tab="serp">SERP Features</div>\n'
    '  <div class="tab" data-tab="movers">Movers &amp; Shakers</div>\n'
    '</div>'
)
new_tabs = (
    '<div class="tabs" id="tabsBar">\n'
    '  <div class="tab active" data-tab="overview">Dashboard</div>\n'
    '  <div class="tab" data-tab="analytics">Analytics</div>\n'
    '  <div class="tab" data-tab="brand">Brand Analysis</div>\n'
    '  <div class="tab" data-tab="anonymized">Anonymized Queries</div>\n'
    '  <div class="tab" data-tab="urls">URL Performance</div>\n'
    '  <div class="tab" data-tab="keywords">Keyword Performance</div>\n'
    '  <div class="tab" data-tab="countries">Country Overview</div>\n'
    '  <div class="tab" data-tab="domains">Domain Comparison</div>\n'
    '  <div class="tab" data-tab="devices">Device & Search Type</div>\n'
    '  <div class="tab" data-tab="comparison">Date Comparison</div>\n'
    '  <div class="tab" data-tab="serp">SERP Features</div>\n'
    '  <div class="tab" data-tab="movers">Movers &amp; Shakers</div>\n'
    '</div>'
)
assert old_tabs in html, "Tab block not found in index.html"
html = html.replace(old_tabs, new_tabs, 1)
print("Tab order updated.")

# ─── 2. Brand Analysis HTML tab ───
brand_html = '''
  <!-- BRAND ANALYSIS -->
  <div class="tab-content" id="tab-brand">
    <div class="filters">
      <div class="filter-group"><label>Market</label><select id="brand-market-filter"><option value="All Markets">All Markets</option></select></div>
      <div class="filter-group"><label>Compare</label>
        <select id="brand-period-compare">
          <option value="7">Last 7 days vs Previous 7 days</option>
          <option value="14">Last 14 days vs Previous 14 days</option>
          <option value="30" selected>Last 30 days vs Previous 30 days</option>
          <option value="0">All time (no comparison)</option>
        </select>
      </div>
    </div>
    <div id="brand-kpis"></div>
    <div class="grid-2">
      <div class="card"><h3>Brand vs Non-Brand Impressions (Daily)</h3><div class="chart-container tall" id="chart-brand-impressions"></div></div>
      <div class="card"><h3>Brand vs Non-Brand Clicks (Daily)</h3><div class="chart-container tall" id="chart-brand-clicks"></div></div>
    </div>
    <div class="card"><h3>Brand Share Trend (%)</h3><div class="chart-container" id="chart-brand-share"></div></div>
    <div class="card"><h3>Brand vs Non-Brand by Market</h3><div class="table-wrap" id="brand-market-table"></div></div>
    <div class="grid-2">
      <div class="card"><h3>Top Brand Queries</h3><div class="table-wrap" id="brand-top-brand"></div></div>
      <div class="card"><h3>Top Non-Brand Queries</h3><div class="table-wrap" id="brand-top-nonbrand"></div></div>
    </div>
  </div>

  <!-- MOVERS & SHAKERS -->'''

old_movers_comment = "\n  <!-- MOVERS & SHAKERS -->"
assert old_movers_comment in html, "Movers comment not found"
html = html.replace(old_movers_comment, brand_html, 1)
print("Brand Analysis HTML tab added.")

# ─── 3. populateFilters — add brand-market-filter ───
old_populate_end = (
    "  const ga4Sel = document.getElementById('ga4-market-filter');\n"
    "  if (ga4Sel) { markets.forEach(m => { const o = document.createElement('option'); o.value = m.market; o.textContent = m.flag+' '+m.market; ga4Sel.appendChild(o); }); }\n"
    "}"
)
new_populate_end = (
    "  const ga4Sel = document.getElementById('ga4-market-filter');\n"
    "  if (ga4Sel) { markets.forEach(m => { const o = document.createElement('option'); o.value = m.market; o.textContent = m.flag+' '+m.market; ga4Sel.appendChild(o); }); }\n"
    "  const brandSel = document.getElementById('brand-market-filter');\n"
    "  if (brandSel) { markets.forEach(m => { const o = document.createElement('option'); o.value = m.market; o.textContent = m.flag+' '+m.market; brandSel.appendChild(o); }); }\n"
    "}"
)
assert old_populate_end in html, "populateFilters end not found"
html = html.replace(old_populate_end, new_populate_end, 1)
print("populateFilters updated.")

# ─── 4. initApp — add renderBrandAnalysis calls ───
old_init_end = (
    "  document.getElementById('ga4-event-compare').addEventListener('change', renderEventTable);\n"
    "}"
)
new_init_end = (
    "  document.getElementById('ga4-event-compare').addEventListener('change', renderEventTable);\n"
    "  renderBrandAnalysis();\n"
    "  document.getElementById('brand-market-filter').addEventListener('change', renderBrandAnalysis);\n"
    "  document.getElementById('brand-period-compare').addEventListener('change', renderBrandAnalysis);\n"
    "}"
)
assert old_init_end in html, "initApp end not found"
html = html.replace(old_init_end, new_init_end, 1)
print("initApp updated.")

# ─── 5. renderBrandAnalysis JS function ───
render_brand_js = r"""
// ═══════════════════════════════════════════
// BRAND ANALYSIS
// ═══════════════════════════════════════════
function renderBrandAnalysis() {
  const bd = DATA.brand_analysis;
  if (!bd) {
    document.getElementById('brand-kpis').innerHTML = '<div class="card"><p style="color:var(--muted);padding:16px">Brand analysis data not available. Run preprocess.py to generate.</p></div>';
    return;
  }

  const market = document.getElementById('brand-market-filter').value;
  const period = parseInt(document.getElementById('brand-period-compare').value);

  const allDates = Object.keys(bd.daily || {}).sort();

  function getDayVals(d) {
    const dayData = bd.daily[d] || {};
    if (market === 'All Markets') {
      let bI=0, bC=0, nbI=0, nbC=0;
      Object.values(dayData).forEach(mv => {
        bI += (mv.brand||{}).impressions||0;
        bC += (mv.brand||{}).clicks||0;
        nbI += (mv.nonbrand||{}).impressions||0;
        nbC += (mv.nonbrand||{}).clicks||0;
      });
      return {bI, bC, nbI, nbC};
    } else {
      const mv = dayData[market] || {};
      return {
        bI: (mv.brand||{}).impressions||0,
        bC: (mv.brand||{}).clicks||0,
        nbI: (mv.nonbrand||{}).impressions||0,
        nbC: (mv.nonbrand||{}).clicks||0
      };
    }
  }

  let curDates, cmpDates;
  if (period === 0) {
    curDates = allDates; cmpDates = [];
  } else {
    curDates = allDates.slice(-period);
    cmpDates = allDates.slice(-period*2, -period);
  }

  function sumDates(dates) {
    return dates.reduce((acc, d) => {
      const v = getDayVals(d);
      return {bI: acc.bI+v.bI, bC: acc.bC+v.bC, nbI: acc.nbI+v.nbI, nbC: acc.nbC+v.nbC};
    }, {bI:0, bC:0, nbI:0, nbC:0});
  }

  const cur = sumDates(curDates);
  const cmp = cmpDates.length ? sumDates(cmpDates) : null;

  const curShare = (cur.bI+cur.nbI) > 0 ? (cur.bI/(cur.bI+cur.nbI)*100).toFixed(1) : '0.0';
  const cmpShare = cmp && (cmp.bI+cmp.nbI) > 0 ? (cmp.bI/(cmp.bI+cmp.nbI)*100).toFixed(1) : null;
  const curBrandCTR = cur.bI > 0 ? (cur.bC/cur.bI*100).toFixed(2) : '0.00';
  const curNBCTR = cur.nbI > 0 ? (cur.nbC/cur.nbI*100).toFixed(2) : '0.00';

  function kpiDelta(a, b) {
    if (b === null || b === undefined || b === 0) return '';
    const d = ((a-b)/b*100).toFixed(1);
    return `<div class="delta ${d>0?'up':'down'}">${d>0?'+':''}${d}%</div>`;
  }

  document.getElementById('brand-kpis').innerHTML = `
    <div class="kpi-row">
      <div class="kpi"><div class="label">Brand Impressions</div><div class="value blue">${fmtN(cur.bI)}</div>${kpiDelta(cur.bI, cmp?.bI)}</div>
      <div class="kpi"><div class="label">Non-Brand Impressions</div><div class="value orange">${fmtN(cur.nbI)}</div>${kpiDelta(cur.nbI, cmp?.nbI)}</div>
      <div class="kpi"><div class="label">Brand Clicks</div><div class="value green">${fmtN(cur.bC)}</div>${kpiDelta(cur.bC, cmp?.bC)}</div>
      <div class="kpi"><div class="label">Non-Brand Clicks</div><div class="value purple">${fmtN(cur.nbC)}</div>${kpiDelta(cur.nbC, cmp?.nbC)}</div>
      <div class="kpi"><div class="label">Brand Share (Impr)</div><div class="value cyan">${curShare}%</div>${cmpShare ? kpiDelta(parseFloat(curShare), parseFloat(cmpShare)) : ''}</div>
      <div class="kpi"><div class="label">Brand CTR</div><div class="value pink">${curBrandCTR}%</div></div>
      <div class="kpi"><div class="label">Non-Brand CTR</div><div class="value orange">${curNBCTR}%</div></div>
    </div>`;

  const chartLabels = curDates.map(d => d.slice(5));
  const curVals = curDates.map(d => getDayVals(d));

  makeChart('chart-brand-impressions', {
    type: 'bar',
    data: {
      labels: chartLabels,
      datasets: [
        {label:'Brand', data: curVals.map(v=>v.bI), backgroundColor:'rgba(99,102,241,0.8)'},
        {label:'Non-Brand', data: curVals.map(v=>v.nbI), backgroundColor:'rgba(245,158,11,0.7)'}
      ]
    },
    options: {responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'top'}},
      scales:{x:{stacked:true, ticks:{maxRotation:45}}, y:{stacked:true, beginAtZero:true}}}
  });

  makeChart('chart-brand-clicks', {
    type: 'bar',
    data: {
      labels: chartLabels,
      datasets: [
        {label:'Brand', data: curVals.map(v=>v.bC), backgroundColor:'rgba(34,197,94,0.8)'},
        {label:'Non-Brand', data: curVals.map(v=>v.nbC), backgroundColor:'rgba(168,85,247,0.7)'}
      ]
    },
    options: {responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'top'}},
      scales:{x:{stacked:true, ticks:{maxRotation:45}}, y:{stacked:true, beginAtZero:true}}}
  });

  const shareData = allDates.map(d => {
    const v = getDayVals(d);
    return (v.bI+v.nbI) > 0 ? parseFloat((v.bI/(v.bI+v.nbI)*100).toFixed(1)) : null;
  });

  makeChart('chart-brand-share', {
    type: 'line',
    data: {
      labels: allDates.map(d => d.slice(5)),
      datasets: [{
        label:'Brand Share %', data: shareData,
        borderColor:'#6366f1', backgroundColor:'rgba(99,102,241,0.1)',
        fill:true, tension:0.3, pointRadius:2, spanGaps:true
      }]
    },
    options: {responsive:true, maintainAspectRatio:false,
      plugins:{legend:{position:'top'}},
      scales:{y:{beginAtZero:false, ticks:{callback: v => v+'%'}}}}
  });

  const byMarket = bd.by_market || {};
  const mktRows = Object.entries(byMarket)
    .sort((a,b) => (b[1].brand?.impressions||0) - (a[1].brand?.impressions||0))
    .map(([mkt, data]) => {
      const b = data.brand||{}, nb = data.nonbrand||{};
      const tot = (b.impressions||0)+(nb.impressions||0);
      const share = tot > 0 ? ((b.impressions||0)/tot*100).toFixed(1) : '0.0';
      return `<tr><td>${mkt}</td><td>${fmtN(b.impressions||0)}</td><td>${fmtN(b.clicks||0)}</td><td>${(b.ctr||0).toFixed(2)}%</td><td>${fmtN(nb.impressions||0)}</td><td>${fmtN(nb.clicks||0)}</td><td>${(nb.ctr||0).toFixed(2)}%</td><td><strong>${share}%</strong></td></tr>`;
    }).join('');
  document.getElementById('brand-market-table').innerHTML = `
    <table><thead><tr><th>Market</th><th>Brand Impr</th><th>Brand Clicks</th><th>Brand CTR</th><th>NB Impr</th><th>NB Clicks</th><th>NB CTR</th><th>Brand Share</th></tr></thead>
    <tbody>${mktRows}</tbody></table>`;

  function makeQueryTable(rows) {
    return `<table><thead><tr><th>Query</th><th>Impressions</th><th>Clicks</th><th>Avg Pos</th><th>CTR</th><th>Markets</th></tr></thead>
    <tbody>${(rows||[]).slice(0,20).map(r=>`<tr><td>${r.query}</td><td>${fmtN(r.impressions)}</td><td>${fmtN(r.clicks)}</td><td>${r.avg_position.toFixed(1)}</td><td>${r.ctr.toFixed(2)}%</td><td>${r.markets}</td></tr>`).join('')}</tbody></table>`;
  }
  document.getElementById('brand-top-brand').innerHTML = makeQueryTable(bd.top_brand);
  document.getElementById('brand-top-nonbrand').innerHTML = makeQueryTable(bd.top_nonbrand);
}

"""

old_boot = "\n// ─── Boot ───\ninitApp();"
assert old_boot in html, "Boot section not found"
html = html.replace(old_boot, render_brand_js + "// ─── Boot ───\ninitApp();", 1)
print("renderBrandAnalysis JS function added.")

# ─── 6. Inject brand_analysis data ───
with open(ROOT / "data" / "brand_analysis.json", "r", encoding="utf-8") as f:
    brand_data = json.load(f)

brand_injection = f"DATA['brand_analysis'] = {json.dumps(brand_data, ensure_ascii=False)};"

# Remove existing brand_analysis injection if present
html = re.sub(r"DATA\['brand_analysis'\] = \{.*?\};\n?", "", html, flags=re.DOTALL)

# Insert before DATA['movers'] or at end of data block (before // ─── Tab Navigation)
# Find a good anchor: right before // ─── Tab Navigation
anchor = "// ─── Tab Navigation ───"
assert anchor in html, "Tab navigation anchor not found"
html = html.replace(anchor, brand_injection + "\n\n" + anchor, 1)
print("brand_analysis data injected.")

with open(ROOT / "index.html", "w", encoding="utf-8") as f:
    f.write(html)

print(f"\nDone! index.html patched successfully.")
