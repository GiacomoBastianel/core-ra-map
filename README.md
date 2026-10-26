# Core remedial action map

An hour-by-hour map of the remedial actions (RAs) that JAO publishes for the Core capacity calculation region (day-ahead flow-based market coupling), from go-live on 9 June 2022 onwards. Each preventive and curative RA is placed on its substation. Pick a business day and an hour (market time unit) to see which phase-shifter taps, bus-bar splits and line switchings the non-costly RA optimisation (NRAO) applied. For a curative RA, the map also draws the critical network elements (CNECs) it protects and the outages it responds to.

**Live map:** `https://<your-user>.github.io/<this-repo>/` once GitHub Pages is switched on (see below).

## How the data stays up to date

`.github/workflows/update-and-deploy.yml` runs every day at 06:00 Brussels time. It is scheduled in UTC for both summer and winter time, and a first step skips whichever doesn't match 06:00 in Brussels that day. JAO publishes each business day around 10:30 the day before, so the 06:00 run adds the current day. Each run:

1. downloads the preventive and curative RAs from the JAO Publication Tool API (`/core/api/data/pra` and `/core/api/data/cra`), starting from the last day already on the map;
2. merges them into `catalog.json` and `data/YYYY-MM.json`, recomputes the usage statistics and places any new RA or CNEC on the map;
3. commits the changed files and redeploys the site.

A missed or failed run is caught up by the next one. To reload a period, open **Actions → Update data and deploy → Run workflow** and enter the dates.

## Switching on GitHub Pages (one time)

**Settings → Pages → Build and deployment → Source: GitHub Actions.** Then run the workflow once from the Actions tab, or push any commit. The site address appears in the deploy job and in Settings → Pages.

## Files

| Path | Content |
| --- | --- |
| `index.html` | The map. There is no build step: D3 and TopoJSON load from cdnjs. |
| `catalog.json` | RA catalogue (JAO name, TSO, type, substation, usage), substations with coordinates and their source, CNECs with geometry, and a daily summary. |
| `data/YYYY-MM.json` | Hourly RAs per month. Each row is `[hourUTC, reason, preventive, reason, curative]`. Preventive entries are `[ra, tapBefore, tapAfter]`. Curative entries are `[cnec, ra, tapBefore, tapAfter, …]`, with indexes into `catalog.json`. The hour is in hours since 1970-01-01 UTC. |
| `grid.json` | Core grid backdrop (lines between substations) from the JAO Static Grid Model. |
| `europe.topo.json` | Country outlines. |
| `pipeline/update.py` | The updater, with no dependencies beyond Python 3.9+. `--fetch` downloads from JAO, `--raw` merges a saved dump, `--range` prints the days it would load. |
| `pipeline/geo.json` | Substation and line lookup used to place new RAs and CNECs. |
| `pipeline/fetch_snippet.js` | Browser fallback for when JAO refuses requests from GitHub's servers (see below). |

## If the JAO API refuses the workflow

Open https://publicationtool.jao.eu/core/ in a browser and paste `pipeline/fetch_snippet.js` into the developer console. Then run:

```js
jaoDump.run('2026-10-20', '2026-10-27')   // business days, Brussels time
jaoDump.state()                            // repeat until "running" is false
jaoDump.download()                         // saves jao-dump.json.gz
```

Then merge the dump and push:

```sh
python3 pipeline/update.py --site . --raw jao-dump.json.gz
git add catalog.json data && git commit -m "Data update" && git push
```

## Run locally

```sh
python3 -m http.server 8000   # then open http://localhost:8000
```

## Sources

- Preventive and curative remedial actions per hour: [JAO Publication Tool, Core CCR](https://publicationtool.jao.eu/core/).
- List of available remedial actions and the Core grid: [JAO Core Static Grid Model](https://www.jao.eu/static-grid-model), 9th release (March 2026).
- Substation coordinates: © [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, ODbL. Each OSM object was matched by name near the position in the Static Grid Model map. The source of every position is listed in `catalog.json` and in the map.
- Country outlines: [Natural Earth](https://www.naturalearthdata.com/) (public domain).
