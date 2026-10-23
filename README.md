# guernsey-roadworks

The Quarry's Guernsey roadworks map, live at https://roadworks.thequarry.media/.

## How it works

`build_data.py` reads the States of Guernsey Traffic & Highways ArcGIS service
(roadworks.gov.gg) and writes `data/roadworks.json`:

- layer 2 (RoadWorks): road-section geometry for each job
- layer 4 (Projects): description, contractor and "Bus Routes Affected" for each job

It keeps jobs that are Approved, In Progress, Pending (shown as "Awaiting approval")
or Planned, ending today or later and starting within 90 days, and drops diversion
routes. No spreadsheet is needed.

The `update.yml` Action runs every two hours at :17 past, on pushes, and on demand.
It deploys to GitHub Pages from `main` only, and commits `data/roadworks.json` only
when the jobs change (the timestamp alone doesn't count). Branch pushes build the
data to prove the fetch works but never deploy.

## Links into the map

`?q=` search, `?when=today|week|month|all`, `?route=91`, `?types=closure,oneway`,
`?date=2026-11-02`, `?job=P046671-THS1`. The map keeps the URL in step with the
filters, so copying the address bar shares the current view.

## Ghost card

`ghost/roadworks-card.html` is a paste-in HTML card for the Roadworks latest page
that lists today's road closures and links to the map.

## Testing offline

`python build_data.py --fixtures DIR --today YYYY-MM-DD` builds from saved
`layer2.json` and `layer4.json` responses instead of the live API.
