#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
Lokal værhistorikk (offline)

Dette scriptet gjør 3 ting:
  1) Leser alle CSV-filer du legger i mappa importer/
     (stasjonen kan gi TO CSV-er per periode: én for regn og én for resten)
  2) Oppdaterer en permanent historikk i store/weather.parquet
  3) Genererer en liten nettside (index.html) + månedlige datafiler i data/

Bruk (samme flyt hver måned):
  - Kopier nye CSV-filer inn i importer/
  - Kjør: python build_weather_page.py
  - Åpne index.html

Installasjon (én gang):
  pip install pandas pyarrow

Hvis nettleseren blokkerer filtilgang (sjeldent):
  python -m http.server 8000
  åpne: http://localhost:8000
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

# -------------------- Konfig ------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
IMPORT_DIR = BASE_DIR / "importer"
ARCHIVE_DIR = BASE_DIR / "arkiv"
MANUAL_DIR = BASE_DIR / "manuelt"
STORE_DIR = BASE_DIR / "store"
DATA_DIR = BASE_DIR / "data"

PARQUET_FILE = STORE_DIR / "weather.parquet"
SNOW_PARQUET_FILE = STORE_DIR / "snow.parquet"
SNOW_CSV_FILE = MANUAL_DIR / "sno.csv"
SNOW_JSON_FILE = DATA_DIR / "snow.json"
MANIFEST_FILE = DATA_DIR / "manifest.json"
INDEX_HTML = BASE_DIR / "index.html"

POSSIBLE_ENCODINGS = ["utf-8", "latin-1", "cp1252"]

# -------------------- HTML --------------------------------------------------
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vær – historikk</title>
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
  <style>
    :root{--bg:#0b1220;--card:#0f1a2f;--muted:#93a4bf;--border:#1e2a44}
    body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu; background:var(--bg); color:#e6edf7}
    header{padding:16px 18px;border-bottom:1px solid var(--border);display:flex;gap:14px;align-items:center;flex-wrap:wrap}
    header h1{font-size:18px;margin:0;font-weight:650;letter-spacing:.2px}
    .pill{background:var(--card);border:1px solid var(--border);padding:10px 12px;border-radius:14px;display:flex;gap:10px;align-items:center}
    select{background:#0b1326;border:1px solid var(--border);color:#e6edf7;border-radius:10px;padding:8px 10px;font-size:14px}
    .meta{color:var(--muted);font-size:12px}
    main{padding:14px}
    .grid{display:grid;grid-template-columns:1fr;gap:14px}
    .card{background:var(--card);border:1px solid var(--border);border-radius:18px;padding:8px}
    .card h2{font-size:15px;margin:10px 12px}
    .plot{height:360px}
    footer{padding:14px 18px;color:var(--muted);font-size:12px}
  </style>
</head>
<body>
  <header>
    <h1>Vær – historikk</h1>
    <div class="pill">
      <span class="meta">Periode</span>
      <select id="month"></select>
      <span class="meta" id="info"></span>
    </div>
  </header>

  <main>
    <div class="grid">
      <section class="card">
        <h2>Temperatur (°C)</h2>
        <div id="temp" class="plot"></div>
        <div id="temp_stats" class="meta" style="padding:0 12px 10px 12px"></div>
      </section>
      
      <section class="card">
        <h2>Vind (m/s)</h2>
        <div id="wind" class="plot"></div>
        <div id="wind_stats" class="meta" style="padding:0 12px 10px 12px"></div>
      </section>
      <section class="card">
        <h2>Regn (mm per intervall)</h2>
        <div id="rain" class="plot"></div>
        <div id="rain_stats" class="meta" style="padding:0 12px 10px 12px"></div>
        <div id="rain_daily" class="meta" style="padding:0 12px 12px 12px; line-height:1.6"></div>
      </section>
      <section class="card">
        <h2>Luftfuktighet (%RH)</h2>
        <div id="hum" class="plot"></div>
        <div id="hum_stats" class="meta" style="padding:0 12px 10px 12px"></div>
      </section>
      <section class="card">
        <h2>Snødybde (cm)</h2>
        <div class="meta" style="padding:0 12px 8px 12px">
          Visning: 
          <select id="snow_mode">
            <option value="follow">Følg valgt periode</option>
            <option value="all">Hele snøhistorikken</option>
          </select>
        </div>
        <div id="snow" class="plot"></div>
        <div id="snow_stats" class="meta" style="padding:0 12px 10px 12px"></div>
      </section>
    </div>
  </main>

  <footer>
    Zoom med mus/fingre. Hold musepekeren over vind-grafen for vindretning (WindHeading).
  </footer>

  <script>
    async function loadJSON(path){
      const r = await fetch(path);
      if(!r.ok) throw new Error('Kunne ikke laste ' + path);
      return await r.json();
    }

    function fmtDate(s){
      const d = new Date(s);
      const dd = String(d.getDate()).padStart(2,'0');
      const mm = String(d.getMonth()+1).padStart(2,'0');
      const yyyy = d.getFullYear();
      return `${dd}.${mm}.${yyyy}`;
    }

    const baseLayout = {
      margin:{l:60,r:20,t:10,b:40},
      paper_bgcolor:'#0f1a2f',
      plot_bgcolor:'#0f1a2f',
      font:{color:'#e6edf7'},
      xaxis:{type:'date', gridcolor:'#1e2a44'},
      yaxis:{gridcolor:'#1e2a44'},
      legend:{orientation:'h'}
    };

    function buildSnow(weatherDs, snowDs){
      const mode = document.getElementById('snow_mode').value;
      let x = snowDs.time || [];
      let y = snowDs.snow_cm || [];
      // Tving datotolking (unngå kategori-akse): bruk Date-objekt
      let xd = x.map(t => new Date(t));

      if(mode === 'follow' && weatherDs && weatherDs.time && weatherDs.time.length){
        const t0 = weatherDs.time[0];
        const t1 = weatherDs.time[weatherDs.time.length-1];
        const p0 = Date.parse(t0);
        const p1 = Date.parse(t1);

        const xf = [];
        const yf = [];
        for(let i=0;i<xd.length;i++){
          const pi = xd[i].getTime();
          if(pi>=p0 && pi<=p1){
            xf.push(xd[i]);
            yf.push(y[i]);
          }
        }
        xd = xf; y = yf;
      }

      const has = x.length && y.length;
      const layout = {...baseLayout, xaxis:{...baseLayout.xaxis, type:'date'}, yaxis:{title:'cm'}, bargap:0.75};

      if(mode === 'follow' && weatherDs && weatherDs.time && weatherDs.time.length){
        layout.xaxis = {...layout.xaxis, range:[new Date(weatherDs.time[0]), new Date(weatherDs.time[weatherDs.time.length-1])]} ;
      }

      // Snø: stolper med fargekoder + "label" på siste stolpe
      function snowColor(v){
        if(v==null || isNaN(v)) return 'rgba(0,0,0,0)';
        if(v<=5) return '#22c55e';      // grønn 0-5
        if(v<=10) return '#60a5fa';     // blå 5.1-10
        if(v<=15) return '#facc15';     // gul 10.1-15
        if(v<=20) return '#ef4444';     // rød 15.1-20
        return '#7c3aed';               // lilla 20+
      }
      const snowColors = y.map(snowColor);

      // Dynamisk stolpebreidde basert på tidsrom mellom målingar (i ms)
      const tms = xd.map(d => d.getTime());
      const widths = x.map((_, i) => {
        const prev = (i>0) ? (tms[i] - tms[i-1]) : null;
        const next = (i<x.length-1) ? (tms[i+1] - tms[i]) : null;
        let w;
        if(prev!=null && next!=null) w = Math.min(prev, next);
        else if(prev!=null) w = prev;
        else if(next!=null) w = next;
        else w = 24*3600*1000; // fallback: 1 døgn
        // Gjør litt smalare enn "full periode" så det blir luft mellom stolpane
        // Skaler ned kraftig slik at stolpane ikkje blir "plankar" ved store hol i målingane
        const scaled = w * 0.18;
        const minW = 30*60*1000;          // 30 min
        const maxW = 10*3600*1000;        // maks 10 timar
        return Math.min(maxW, Math.max(minW, scaled));
      });

      // Finn siste gyldige måling (for label)
      let lastIdx = -1;
      for(let i=y.length-1;i>=0;i--){ if(y[i]!=null && !isNaN(y[i])) { lastIdx=i; break; } }

      const traces = [
        {
          x: xd, y,
          name:'Snødybde',
          type:'bar',
          marker:{color: snowColors},
          width: widths,
          hovertemplate:'%{x}<br>%{y:.1f} cm<extra></extra>'
        }
      ];

      if(lastIdx >= 0){
        traces.push({
          x:[xd[lastIdx]],
          y:[y[lastIdx]],
          type:'scatter',
          mode:'text',
          text:[`${(Math.round(y[lastIdx]*10)/10).toFixed( (Math.abs(y[lastIdx] - Math.round(y[lastIdx]))<1e-9) ? 0 : 1)} cm`],
          textposition:'top center',
          hoverinfo:'skip',
          showlegend:false
        });
      }

      Plotly.newPlot('snow', traces, layout, {responsive:true});

      // Enkel stats
      const el = document.getElementById('snow_stats');
      if(!has){
        el.textContent = 'Ingen snømålinger i valgt visning.';
        return;
      }
      let last = null;
      for(let i=y.length-1;i>=0;i--){
        if(y[i]!=null && !isNaN(y[i])){ last = {t:x[i], v:y[i]}; break; }
      }
      if(last){
        el.textContent = `Siste måling: ${Math.round(last.v)} cm (${fmtDT(last.t)})`;
      } else {
        el.textContent = 'Ingen snømålinger i valgt visning.';
      }
    }

    function linkXAxes(divIds){
      // Når du zoomer i én graf, oppdateres de andre.
      const divs = divIds.map(id => document.getElementById(id));
      divs.forEach(src => {
        src.on('plotly_relayout', ev => {
          const r0 = ev['xaxis.range[0]'];
          const r1 = ev['xaxis.range[1]'];
          const autor = ev['xaxis.autorange'];
          if(r0 && r1){
            divs.forEach(dst => { if(dst !== src) Plotly.relayout(dst, {'xaxis.range':[r0,r1]}); });
          } else if(autor){
            divs.forEach(dst => { if(dst !== src) Plotly.relayout(dst, {'xaxis.autorange': true}); });
          }
        });
      });
    }

    function buildPlots(ds, label, isAll){
      setSummaries(ds, label, isAll);

      const x = ds.time;

      // Vind i m/s
      const wind_ms = ds.windspeed_kmh.map(v => (v==null? null : v/3.6));

      // Vindretning hover: viser både grader og (om ønskelig) enkel tekst
      function degToCompass(deg){
        if(deg==null || isNaN(deg)) return '';
        const dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
        const ix = Math.round((((deg%360)+360)%360) / 22.5) % 16;
        return dirs[ix];
      }

      function sum(arr){
        let s = 0;
        for(const v of arr){ if(v!=null && !isNaN(v)) s += v; }
        return s;
      }

      function minMax(arr){
        let mn = null, mx = null;
        for(const v of arr){
          if(v==null || isNaN(v)) continue;
          if(mn==null || v<mn) mn=v;
          if(mx==null || v>mx) mx=v;
        }
        return {min:mn, max:mx};
      }

      function dailyTotals(ds){
        const map = new Map();
        for(let i=0;i<ds.time.length;i++){
          const mm = ds.rain_mm[i];
          if(mm==null || isNaN(mm)) continue;
          const date = ds.time[i].slice(0,10);
          map.set(date, (map.get(date) || 0) + mm);
        }
        return Array.from(map.entries())
          .sort((a,b)=>a[0].localeCompare(b[0]))
          .map(([date,total])=>({date,total}));
      }

      function rainLast24h(ds){
        if(!ds.time.length) return 0;
        const tEnd = Date.parse(ds.time[ds.time.length-1]);
        const tStart = tEnd - 24*3600*1000;
        let s = 0;
        for(let i=0;i<ds.time.length;i++){
          const ti = Date.parse(ds.time[i]);
          if(ti >= tStart && ti <= tEnd){
            const v = ds.rain_mm[i];
            if(v!=null && !isNaN(v)) s += v;
          }
        }
        return s;
      }

      function fmtDT(ts){
        if(!ts) return '';
        const d = new Date(ts);
        const dd = String(d.getDate()).padStart(2,'0');
        const mm = String(d.getMonth()+1).padStart(2,'0');
        const yyyy = d.getFullYear();
        const hh = String(d.getHours()).padStart(2,'0');
        const mi = String(d.getMinutes()).padStart(2,'0');
        return `${dd}.${mm}.${yyyy} ${hh}:${mi}`;
      }

      function tableHtml(title, rowsHtml){
        return `
          <b>${title}</b>
          <div style="overflow:auto;margin-top:6px">
            <table style="border-collapse:collapse;min-width:260px">
              <thead>
                <tr>
                  <th style="text-align:left;border-bottom:1px solid #1e2a44;padding:6px 8px">Dato</th>
                  <th style="text-align:right;border-bottom:1px solid #1e2a44;padding:6px 8px">Nedbør</th>
                </tr>
              </thead>
              <tbody>${rowsHtml}</tbody>
            </table>
          </div>`;
      }

      function setSummaries(ds, label, isAll){
        // Temperatur
        const tmm = minMax(ds.temperature_c);
        let tempText;
        if(tmm.min==null){
          tempText = 'Ingen temperaturdata';
        } else {
          const iMin = ds.temperature_c.findIndex(v=>v!=null && Math.abs(v - tmm.min) < 1e-9);
          const iMax = ds.temperature_c.findIndex(v=>v!=null && Math.abs(v - tmm.max) < 1e-9);
          const tMinAt = (iMin>=0)? ` (${fmtDT(ds.time[iMin])})` : '';
          const tMaxAt = (iMax>=0)? ` (${fmtDT(ds.time[iMax])})` : '';
          tempText = `Laveste: ${tmm.min.toFixed(1)} °C${tMinAt} • Høyeste: ${tmm.max.toFixed(1)} °C${tMaxAt}`;
        }
        document.getElementById('temp_stats').textContent = (label? `${label} • `:'') + tempText;

        // Vind
        const wms = ds.windspeed_kmh.map(v => (v==null? null : v/3.6));
        const wmm = minMax(wms);
        let maxDir = '';
        if(wmm.max!=null){
          const iMax = wms.findIndex(v => v!=null && Math.abs(v - wmm.max) < 1e-9);
          const hd = (iMax>=0) ? ds.windheading[iMax] : null;
          if(hd!=null && !isNaN(hd)) maxDir = ` (${degToCompass(hd)})`;
        }
        let windText;
        if(wmm.min==null){
          windText = 'Ingen vinddata';
        } else {
          let maxAt = '';
          if(wmm.max!=null){
            const iMax = wms.findIndex(v => v!=null && Math.abs(v - wmm.max) < 1e-9);
            if(iMax>=0) maxAt = ` (${fmtDT(ds.time[iMax])})`;
          }
          windText = `Laveste: ${wmm.min.toFixed(1)} m/s • Høyeste: ${wmm.max.toFixed(1)} m/s${maxDir}${maxAt}`;
        }
        document.getElementById('wind_stats').textContent = (label? `${label} • `:'') + windText;

        // Luftfuktighet
        const hmm = minMax(ds.humidity_rh);
        const humText = (hmm.min==null) ? 'Ingen fuktdata' : `Laveste: ${hmm.min.toFixed(0)} % • Høyeste: ${hmm.max.toFixed(0)} %`;
        document.getElementById('hum_stats').textContent = (label? `${label} • `:'') + humText;

        // Regn bokser
        const monthSum = sum(ds.rain_mm);
        const last24 = rainLast24h(ds);
        const rainText = `Siste 24 t: ${last24.toFixed(1)} mm • Sum: ${monthSum.toFixed(1)} mm`;
        document.getElementById('rain_stats').textContent = (label? `${label} • `:'') + rainText;

        // Regn-tabell
        const rainDailyEl = document.getElementById('rain_daily');
        const daily = dailyTotals(ds);
        if(!daily.length){
          rainDailyEl.textContent = 'Ingen regndata å summere per døgn.';
          return;
        }

        if(isAll){
          const top = daily.slice().sort((a,b)=>(b.total||0)-(a.total||0)).slice(0,20);
          const rows = top.map(x=>`<tr><td>${x.date}</td><td style="text-align:right">${(x.total||0).toFixed(1)} mm</td></tr>`).join('');
          rainDailyEl.innerHTML = tableHtml('20 våteste døgn', rows);
        } else {
          const monthStr = ds.time[0].slice(0,7); // YYYY-MM
          const year = parseInt(monthStr.slice(0,4), 10);
          const mon = parseInt(monthStr.slice(5,7), 10);
          const daysInMonth = new Date(year, mon, 0).getDate();
          const totalsMap = new Map(daily.map(x=>[x.date, x.total]));
          let rows = '';
          for(let d=1; d<=daysInMonth; d++){
            const dd = String(d).padStart(2,'0');
            const date = `${monthStr}-${dd}`;
            const total = totalsMap.has(date) ? totalsMap.get(date) : 0;
            rows += `<tr><td>${date}</td><td style="text-align:right">${(total||0).toFixed(1)} mm</td></tr>`;
          }
          rainDailyEl.innerHTML = tableHtml(`Regn per døgn (${monthStr})`, rows);
        }
      }

      const windDirTxt = (ds.windheading || []).map(v => (v==null? '' : `Retning: ${degToCompass(v)} (${Math.round(v)}°)`));

      // Temperatur: to farger (<=0 lyseblå, >0 lyserød)
      const t = ds.temperature_c;
      const tCold = t.map(v => (v==null ? null : (v<=0 ? v : null)));
      const tWarm = t.map(v => (v==null ? null : (v>0 ? v : null)));

      Plotly.newPlot('temp', [
        {x, y: tCold, name:'Temperatur ≤ 0°C', mode:'lines', line:{color:'#7dd3fc'}, hovertemplate:'%{x}<br>%{y:.1f} °C<extra></extra>'},
        {x, y: tWarm, name:'Temperatur > 0°C', mode:'lines', line:{color:'#fda4af'}, hovertemplate:'%{x}<br>%{y:.1f} °C<extra></extra>'}
      ], {...baseLayout, yaxis:{title:'°C'}}, {responsive:true});

      Plotly.newPlot('hum', [
        {x, y: ds.humidity_rh, name:'Fukt', mode:'lines', hovertemplate:'%{x}<br>%{y:.0f} %RH<extra></extra>'}
      ], {...baseLayout, yaxis:{title:'%RH'}}, {responsive:true});

      // Vind: linje (lett å treffe) + bevar fargekode via markører
      // NB: ekstremiteter bevares i import (maks per minutt) – se Python-delen.
      const windMarkerColors = wind_ms.map(v => {
        if(v==null || isNaN(v)) return 'rgba(0,0,0,0)';
        if(v<=5) return '#60a5fa';
        if(v<=11) return '#facc15';
        if(v<=18) return '#22c55e';
        if(v<=24) return '#ef4444';
        return '#7c3aed';
      });

      Plotly.newPlot('wind', [
        {
          x, y: wind_ms,
          name:'Vind',
          mode:'lines+markers',
          marker:{color: windMarkerColors, size:4},
          line:{width:1.5, color:'#94a3b8'},
          customdata: windDirTxt,
          hovertemplate:'%{x}<br>%{y:.1f} m/s<br>%{customdata}<extra></extra>'
        }
      ], {...baseLayout, yaxis:{title:'m/s'}}, {responsive:true});

      Plotly.newPlot('rain', [
        {x, y: ds.rain_mm, name:'Regn', mode:'lines', hovertemplate:'%{x}<br>%{y:.2f} mm<extra></extra>'}
      ], {...baseLayout, yaxis:{title:'mm'}}, {responsive:true});

      linkXAxes(['temp','hum','wind','rain']);
    }

    async function main(){
      const manifest = await loadJSON('data/manifest.json');
      const snowDs = await loadJSON('data/' + (manifest.snow || 'snow.json'));
      window.__snowDs = snowDs;
      const sel = document.getElementById('month');
      const info = document.getElementById('info');

      // Legg til "Alle" øverst (gjør det mulig å pan'e på tvers av måneder)
      const optAll = document.createElement('option');
      optAll.value = '__ALL__';
      optAll.textContent = 'Alle (samlet)';
      sel.appendChild(optAll);

      manifest.months.forEach(m => {
        const opt = document.createElement('option');
        opt.value = m.file;
        opt.textContent = m.label;
        sel.appendChild(opt);
      });

      async function loadSelected(){
        const ds = await (async ()=>{
          if(sel.value === '__ALL__'){
            const parts = await Promise.all(manifest.months.map(m => loadJSON('data/' + m.file)));
            const merged = {time:[], temperature_c:[], humidity_rh:[], windspeed_kmh:[], windheading:[], rain_mm:[]};
            for(const p of parts){
              merged.time.push(...(p.time||[]));
              merged.temperature_c.push(...(p.temperature_c||[]));
              merged.humidity_rh.push(...(p.humidity_rh||[]));
              merged.windspeed_kmh.push(...(p.windspeed_kmh||[]));
              merged.windheading.push(...(p.windheading||[]));
              merged.rain_mm.push(...(p.rain_mm||[]));
            }
            const idx = merged.time.map((t,i)=>[Date.parse(t),i]).sort((a,b)=>a[0]-b[0]).map(x=>x[1]);
            const pick = (arr)=>idx.map(i=>arr[i]);
            return {
              time: pick(merged.time),
              temperature_c: pick(merged.temperature_c),
              humidity_rh: pick(merged.humidity_rh),
              windspeed_kmh: pick(merged.windspeed_kmh),
              windheading: pick(merged.windheading),
              rain_mm: pick(merged.rain_mm)
            };
          }
          return await loadJSON('data/' + sel.value);
        })();

        window.__weatherDs = ds;

        if(!ds.time || !ds.time.length){
          info.textContent = 'Tomt datasett';
          return;
        }
        info.textContent = `${fmtDate(ds.time[0])} – ${fmtDate(ds.time[ds.time.length-1])} • ${ds.time.length.toLocaleString('no-NO')} punkt` + (sel.value === '__ALL__' ? ' • (samlet)' : '');

        const isAll = (sel.value === '__ALL__');
        const label = isAll ? 'Alle (samlet)' : sel.options[sel.selectedIndex].textContent;
        buildPlots(ds, label, isAll);
        buildSnow(ds, window.__snowDs || {time:[], snow_cm:[]});
      }

      sel.addEventListener('change', loadSelected);

      // Snø: egen visning (uavhengig av pan/zoom i andre grafer)
      document.getElementById('snow_mode').addEventListener('change', ()=>{
        buildSnow(window.__weatherDs || {time:[]}, window.__snowDs || {time:[], snow_cm:[]});
      });

      if(manifest.months.length){
        sel.value = '__ALL__';
        await loadSelected();
      } else {
        info.textContent = 'Ingen data ennå – legg CSV-filer i importer/ og kjør scriptet.';
      }
    }

    main().catch(err => {
      document.getElementById('info').textContent = err.message;
      console.error(err);
    });
  </script>
</body>
</html>
"""


# -------------------- Import/normalisering ----------------------------------
@dataclass
class Parsed:
    df: pd.DataFrame
    kind: str  # "met" eller "rain"


def read_csv_any_encoding(path: Path) -> pd.DataFrame:
    last_err: Exception | None = None
    for enc in POSSIBLE_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc, sep=None, engine="python")
        except Exception as e:
            last_err = e
    raise last_err or RuntimeError("Kunne ikke lese CSV")


def find_col(cols: list[str], keywords: list[str]) -> str | None:
    low = {c: c.lower() for c in cols}
    for c, cl in low.items():
        for kw in keywords:
            if kw in cl:
                return c
    return None


def parse_station_csv(path: Path) -> Parsed:
    df = read_csv_any_encoding(path)

    if "Time" not in df.columns:
        # fallback for varianter som har Date + Time
        date_col = find_col(list(df.columns), ["date"])
        time_col = find_col(list(df.columns), ["time"])
        if date_col and time_col:
            df["Time"] = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str), errors="coerce")
        else:
            raise ValueError(f"Fant ikke Time i {path.name}. Kolonner: {list(df.columns)}")
    else:
        df["Time"] = pd.to_datetime(df["Time"], errors="coerce")

    df = df.dropna(subset=["Time"]).copy().sort_values("Time")
    cols = list(df.columns)

    # Regn
    rain_rate_col = find_col(cols, ["rain rate", "mm/h", "mm per h", "mm pr h"])
    rain_col = find_col(cols, ["rain", "precip", "nedbør", "nedbor"])  # bred match

    # Met
    temp_col = find_col(cols, ["temperature", "temp"])
    hum_col = find_col(cols, ["humidity", "rh%", "rh"])
    wind_col = find_col(cols, ["windspeed", "wind speed", "km/h", "kmh"])
    heading_col = find_col(cols, ["windheading", "wind heading", "winddir", "direction", "retning"])

    # Heuristikk for regnfil: har rain eller rain rate
    if rain_col or rain_rate_col:
        out = pd.DataFrame({"Time": df["Time"]})
        if rain_rate_col:
            out["rain_rate_mmh"] = pd.to_numeric(df[rain_rate_col], errors="coerce")
        if rain_col:
            out["rain_raw"] = pd.to_numeric(df[rain_col], errors="coerce")
        return Parsed(out, "rain")

    out = pd.DataFrame({"Time": df["Time"]})
    if temp_col:
        out["temperature_c"] = pd.to_numeric(df[temp_col], errors="coerce")
    if hum_col:
        out["humidity_rh"] = pd.to_numeric(df[hum_col], errors="coerce")
    if wind_col:
        # NB: om vindkolonnen i fila heter "WindSpeed Km/h" vil den matches av windspeed.
        out["windspeed_kmh"] = pd.to_numeric(df[wind_col], errors="coerce")
    if heading_col:
        # WindHeading kan være grader (0-360) ELLER kompass (N, SW, W ...)
        s = df[heading_col].astype(str).str.strip()
        # Fjern evt gradtegn og annet støy
        s_num = s.str.replace("°", "", regex=False)
        s_num = s_num.str.replace(r"[^0-9.+-]", "", regex=True)
        nums = pd.to_numeric(s_num, errors="coerce")
        if nums.notna().any():
            out["windheading"] = nums
        else:
            compass = {
                "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
                "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
                "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
                "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
            }
            key = s.str.upper().str.replace(" ", "", regex=False)
            out["windheading"] = key.map(compass)

    return Parsed(out, "met")


def rain_to_interval_mm(rain_df: pd.DataFrame) -> pd.Series:
    rain_df = rain_df.sort_values("Time")

    if "rain_rate_mmh" in rain_df.columns and rain_df["rain_rate_mmh"].notna().any():
        rate = rain_df["rain_rate_mmh"].fillna(0)
        dt = rain_df["Time"].diff().dt.total_seconds().div(3600.0).fillna(0)
        mm = rate * dt
        mm[mm < 0] = 0
        return mm

    if "rain_raw" in rain_df.columns and rain_df["rain_raw"].notna().any():
        raw = rain_df["rain_raw"].astype(float)
        dif = raw.diff()
        nondec_ratio = (dif.fillna(0) >= 0).mean()
        if nondec_ratio > 0.90:
            # akkumulerende teller (reset gir negativ diff)
            dif = dif.where(dif >= 0, 0)
            return dif.fillna(0)
        # ser mer ut som intervallverdi allerede
        return raw

    return pd.Series([None] * len(rain_df), index=rain_df.index, dtype="float")


# -------------------- Pipeline ------------------------------------------------

def ensure_dirs() -> None:
    for d in [IMPORT_DIR, ARCHIVE_DIR, MANUAL_DIR, STORE_DIR, DATA_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def load_master() -> pd.DataFrame:
    if PARQUET_FILE.exists():
        return pd.read_parquet(PARQUET_FILE)
    return pd.DataFrame(columns=["Time", "temperature_c", "humidity_rh", "windspeed_kmh", "windheading", "rain_mm"])


def load_snow_master() -> pd.DataFrame:
    if SNOW_PARQUET_FILE.exists():
        return pd.read_parquet(SNOW_PARQUET_FILE)
    return pd.DataFrame(columns=["Time", "snow_cm"])


def save_master(df: pd.DataFrame) -> None:
    df = df.sort_values("Time")
    df.to_parquet(PARQUET_FILE, index=False)


def save_snow_master(df: pd.DataFrame) -> None:
    df = df.sort_values("Time")
    df.to_parquet(SNOW_PARQUET_FILE, index=False)


def ingest_import_folder() -> tuple[int, int, str]:
    master = load_master()

    files = sorted([p for p in IMPORT_DIR.glob("*.csv") if p.is_file()])
    if not files:
        print("Fant ingen CSV i importer/")
        return (0, 0, "")

    met_parts: list[pd.DataFrame] = []
    rain_parts: list[pd.DataFrame] = []

    for f in files:
        parsed = parse_station_csv(f)
        if parsed.kind == "met":
            met_parts.append(parsed.df)
        else:
            rain_parts.append(parsed.df)

    if met_parts:
        met = pd.concat(met_parts, ignore_index=True).drop_duplicates(subset=["Time"]).sort_values("Time")
    else:
        met = pd.DataFrame(columns=["Time", "temperature_c", "humidity_rh", "windspeed_kmh", "windheading"])

    if rain_parts:
        rain = pd.concat(rain_parts, ignore_index=True).drop_duplicates(subset=["Time"]).sort_values("Time")
        rain_mm = rain_to_interval_mm(rain)
        rain2 = pd.DataFrame({"Time": rain["Time"].values, "rain_mm": rain_mm.values})
    else:
        rain2 = pd.DataFrame(columns=["Time", "rain_mm"])

    # --- Tids-match mellom met og regn ---
    # Noen stasjoner logger med litt ulike sekunder i de to filene.
    # Vi runder derfor tidspunkt til nærmeste minutt og slår sammen på det.
    if not met.empty:
        met = met.copy()
        met["Time_key"] = pd.to_datetime(met["Time"], errors="coerce").dt.floor("min")
        met = met.dropna(subset=["Time_key"]).sort_values("Time_key")
        # ved flere punkter same minutt:
        # - vind: ta maks (bevar ekstremar)
        # - øvrige felt: ta siste
        met = met.sort_values("Time")
        if "windspeed_kmh" in met.columns and met["windspeed_kmh"].notna().any():
            idx_max = met.groupby("Time_key")["windspeed_kmh"].idxmax()
            wind_max = met.loc[idx_max, ["Time_key", "windspeed_kmh", "windheading"]]
            others = met.drop(columns=["windspeed_kmh", "windheading"], errors="ignore")
            others = others.drop_duplicates(subset=["Time_key"], keep="last")
            met = pd.merge(others, wind_max, on="Time_key", how="left")
        else:
            met = met.drop_duplicates(subset=["Time_key"], keep="last")
    else:
        met["Time_key"] = pd.Series(dtype="datetime64[ns]")

    if not rain2.empty:
        rain2 = rain2.copy()
        rain2["Time_key"] = pd.to_datetime(rain2["Time"], errors="coerce").dt.floor("min")
        rain2 = rain2.dropna(subset=["Time_key"]).sort_values("Time_key")
        # ved flere punkter samme minutt: summer regn
        rain2 = rain2.groupby("Time_key", as_index=False)["rain_mm"].sum(min_count=1)
    else:
        rain2["Time_key"] = pd.Series(dtype="datetime64[ns]")

    if not met.empty and not rain2.empty:
        merged = pd.merge(met.drop(columns=["Time"]), rain2, on="Time_key", how="outer")
    elif not met.empty:
        merged = met.drop(columns=["Time"]).copy()
        merged["rain_mm"] = pd.NA
    else:
        merged = rain2.copy()
        merged["temperature_c"] = pd.NA
        merged["humidity_rh"] = pd.NA
        merged["windspeed_kmh"] = pd.NA
        merged["windheading"] = pd.NA

    # Sett Time til Time_key (minutt-oppløsning)
    merged["Time"] = merged["Time_key"]
    merged = merged.drop(columns=["Time_key"])

    for c in ["temperature_c", "humidity_rh", "windspeed_kmh", "windheading", "rain_mm"]:
        if c not in merged.columns:
            merged[c] = pd.NA

    merged = merged[["Time", "temperature_c", "humidity_rh", "windspeed_kmh", "windheading", "rain_mm"]]
    merged["Time"] = pd.to_datetime(merged["Time"], errors="coerce")
    merged = merged.dropna(subset=["Time"]).sort_values("Time")

    before = len(master)
    imported_rows = len(merged)

    master2 = pd.concat([master, merged], ignore_index=True)
    master2 = master2.sort_values("Time").drop_duplicates(subset=["Time"], keep="last")

    after = len(master2)
    dedup_removed = (before + imported_rows) - after

    # Fyll manglende vindretning med siste kjente (gir bedre hover i graf)
    try:
        master2 = master2.sort_values("Time")
        master2["windheading"] = master2["windheading"].ffill()
    except Exception:
        pass
    save_master(master2)

    # Periode-navn basert på alle tider vi nettopp importerte
    t0 = pd.to_datetime(merged["Time"].min())
    t1 = pd.to_datetime(merged["Time"].max())
    bundle = "unknown"
    if not pd.isna(t0) and not pd.isna(t1):
        bundle = f"{t0.strftime('%Y-%m-%d')}_{t1.strftime('%Y-%m-%d')}"

    # Flytt CSV-er til arkiv med ryddig navn
    for f in files:
        safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in f.stem)
        target = ARCHIVE_DIR / f"{bundle}__{safe}.csv"
        i = 2
        while target.exists():
            target = ARCHIVE_DIR / f"{bundle}__{safe}({i}).csv"
            i += 1
        shutil.move(str(f), str(target))

    print(f"Importerte {len(files)} filer og arkiverte dem")
    return (imported_rows, dedup_removed, bundle)


def generate_monthly_json() -> list[dict]:
    master = load_master()
    if master.empty:
        return []

    master = master.copy()
    master["Time"] = pd.to_datetime(master["Time"], errors="coerce")
    master = master.dropna(subset=["Time"]).sort_values("Time")
    master["month"] = master["Time"].dt.strftime("%Y-%m")

    months: list[dict] = []
    for m, g in master.groupby("month"):
        g = g.sort_values("Time")
        out = {
            "time": g["Time"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
            "temperature_c": [None if pd.isna(v) else float(v) for v in g["temperature_c"]],
            "humidity_rh": [None if pd.isna(v) else float(v) for v in g["humidity_rh"]],
            "windspeed_kmh": [None if pd.isna(v) else float(v) for v in g["windspeed_kmh"]],
            "windheading": [None if pd.isna(v) else float(v) for v in g["windheading"]],
            "rain_mm": [None if pd.isna(v) else float(v) for v in g["rain_mm"]]
        }

        fname = f"{m}.json"
        (DATA_DIR / fname).write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        months.append({"label": m, "file": fname})

    months.sort(key=lambda x: x["label"], reverse=True)
    return months


def ingest_snow_file() -> int:
    """Les manuelt/sno.csv og oppdater store/snow.parquet.

    Format (anbefalt):
      Date,Snow_cm
      2026-01-05,45

    - Én rad per dato.
    - Hvis samme dato finnes flere ganger, vinner siste (overskriver).
    """
    if not SNOW_CSV_FILE.exists():
        return 0

    df = read_csv_any_encoding(SNOW_CSV_FILE)

    # Finn kolonner
    date_col = None
    for c in df.columns:
        if str(c).strip().lower() in ["date", "dato"]:
            date_col = c
            break
    snow_col = None
    for c in df.columns:
        if str(c).strip().lower() in ["snow_cm", "snow", "snø_cm", "sno_cm", "snø", "sno"]:
            snow_col = c
            break

    if date_col is None or snow_col is None:
        raise ValueError(f"manuelt/sno.csv må ha kolonnene Date og Snow_cm. Fant: {list(df.columns)}")

    out = pd.DataFrame()
    out["Time"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    # Støtter desimal med punkt (12.4). Hvis noen likevel skriver 12,4, prøv å konvertere.
    s = df[snow_col].astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    out["snow_cm"] = pd.to_numeric(s, errors="coerce")
    out = out.dropna(subset=["Time"]).sort_values("Time")

    if out.empty:
        return 0

    # Overskriv per dato (siste vinner)
    out = out.dropna(subset=["snow_cm"], how="all")
    out = out.drop_duplicates(subset=["Time"], keep="last")

    master = load_snow_master()
    before = len(master)
    master2 = pd.concat([master, out], ignore_index=True)
    master2 = master2.sort_values("Time").drop_duplicates(subset=["Time"], keep="last")
    save_snow_master(master2)

    return len(out)


def generate_snow_json() -> None:
    master = load_snow_master()
    if master.empty:
        SNOW_JSON_FILE.write_text(json.dumps({"time": [], "snow_cm": []}, ensure_ascii=False), encoding="utf-8")
        return

    master = master.copy()
    master["Time"] = pd.to_datetime(master["Time"], errors="coerce")
    master = master.dropna(subset=["Time"]).sort_values("Time")

    out = {
        "time": master["Time"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
        "snow_cm": [None if pd.isna(v) else float(v) for v in master["snow_cm"]],
    }
    SNOW_JSON_FILE.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")


def write_manifest(months: list[dict]) -> None:
    # Snødata ligger alltid i data/snow.json (uavhengig av måneder)
    MANIFEST_FILE.write_text(json.dumps({"months": months, "snow": "snow.json"}, ensure_ascii=False, indent=2), encoding="utf-8")


def write_index_html() -> None:
    INDEX_HTML.write_text(HTML_TEMPLATE, encoding="utf-8")


def main() -> None:
    ensure_dirs()

    imported_rows, dedup_removed, bundle = ingest_import_folder()

    snow_rows = 0
    try:
        snow_rows = ingest_snow_file()
    except Exception as e:
        print("⚠️ Snøimport feil:", e)

    months = generate_monthly_json()
    generate_snow_json()
    write_manifest(months)
    write_index_html()

    print("Oppdatert værhistorikk!")
    if bundle:
        print(f"- Siste import-periode: {bundle}")
    print("- Importerte rader:", imported_rows)
    print("- Fjernet duplikater:", dedup_removed)
    print(f"- Masterlager: {PARQUET_FILE}")
    print(f"- Nettside: {INDEX_HTML}")
    print(f"- Måneder tilgjengelig: {len(months)}")


if __name__ == "__main__":
    main()
