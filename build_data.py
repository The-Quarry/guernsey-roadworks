#!/usr/bin/env python3
"""
Build data/roadworks.json for roadworks.thequarry.media from the States of
Guernsey Traffic & Highways ArcGIS service. No spreadsheet needed.

  Layer 2 (RoadWorks)  polyline segments per job, one row per road section
  Layer 4 (Projects)   one row per job: description, contractor, bus routes

Usage:
  python build_data.py                      # live API
  python build_data.py --fixtures DIR       # offline, from saved responses
                                            # (layer2.json, layer4.json, layer2_info.json)

Exits non-zero without touching the output file if the fetch fails or the
result looks wrong, so the site keeps serving the last good build.
"""

import argparse, json, math, re, sys, time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from pyproj import Transformer

SERVICE = "https://roadworks.gov.gg/arcgis/rest/services/GSW/MapServer"
LAYER_SEGMENTS = 2
LAYER_PROJECTS = 4
WINDOW_DAYS = 90
CHUNK = 200
OUT = Path("data/roadworks.json")
TZ = ZoneInfo("Europe/London")

# The service reports WKID 102070, which PROJ doesn't know. EPSG:3108
# (ETRS89 / Guernsey Grid) is the published equivalent and lines up with OSM.
SOURCE_CRS = "EPSG:3108"

KEEP_STATUS = {"Approved", "In Progress", "Pending", "Planned (unreserved)"}
ISLAND_BOX = {"lon": (-2.75, -2.45), "lat": (49.40, 49.52)}


# ---------------------------------------------------------------- fetching
def get_json(url, params, tries=4):
    q = url + "?" + urlencode(params)
    for i in range(tries):
        try:
            with urlopen(Request(q, headers={"User-Agent": "thequarry-roadworks/2"}), timeout=60) as r:
                data = json.load(r)
            if "error" in data:
                raise RuntimeError(data["error"])
            return data
        except Exception as e:  # network blip or ArcGIS error
            if i == tries - 1:
                raise
            wait = 5 * (i + 1)
            print(f"  retry in {wait}s after: {e}")
            time.sleep(wait)


def fetch_layer(layer, where, geometry):
    url = f"{SERVICE}/{layer}/query"
    ids = get_json(url, {"where": where, "returnIdsOnly": "true", "f": "json"}).get("objectIds") or []
    feats = []
    for i in range(0, len(ids), CHUNK):
        page = get_json(url, {
            "objectIds": ",".join(map(str, ids[i:i + CHUNK])),
            "outFields": "*",
            "returnGeometry": "true" if geometry else "false",
            "f": "json",
        })
        feats.extend(page.get("features", []))
    print(f"layer {layer}: {len(feats)} rows")
    return feats


# ---------------------------------------------------------------- cleaning
PARTICLES = {"de", "du", "des", "la", "le", "les", "a", "au", "aux", "et", "en", "d", "l", "ès",
             "to", "of", "and", "the", "on", "at"}


def nice_name(s):
    """'ST. PETER PORT - RUE A L`OR' style -> 'Rue a l'Or'."""
    s = (s or "").replace("`", "'").replace("’", "'").strip()
    s = re.sub(r"\bST\.\s*", "ST ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    out = []
    for i, word in enumerate(s.split(" ")):
        parts = []
        for j, p in enumerate(word.split("'")):
            low = p.lower()
            if j > 0 and low == "s":
                parts.append("s")
            elif (i > 0 or j > 0) and low in PARTICLES and not (i == 0 and j == 0):
                parts.append(low)
            else:
                parts.append("-".join(x[:1].upper() + x[1:] for x in low.split("-")))
        out.append("'".join(parts))
    if out:
        out[0] = out[0][:1].upper() + out[0][1:]
    return " ".join(out)


def split_location(loc):
    loc = (loc or "").strip()
    if " - " in loc:
        parish, road = loc.split(" - ", 1)
        return nice_name(road), nice_name(parish)
    return nice_name(loc), ""


PUBLIC_ROUTE = re.compile(r"^(?:\d{1,2}|N\d|P\d)$", re.I)


def parse_bus(text):
    """'LV 1A; 31, 32' -> (['31','32'], ['LV 1A'])  public routes, school services."""
    public, school = [], []
    for tok in re.split(r"[;,]", text or ""):
        tok = re.sub(r"\s+", " ", tok).strip(" -")
        if not tok:
            continue
        if PUBLIC_ROUTE.match(tok):
            tok = tok.upper()
            if tok not in public:
                public.append(tok)
        elif tok not in school:
            school.append(tok)
    public.sort(key=lambda r: (not r[0].isdigit(), int(re.sub(r"\D", "", r) or 0), r))
    return public, school


def split_description(desc):
    desc = (desc or "").strip()
    bus = ""
    m = re.search(r";?\s*Bus Routes Affected:\s*(.*)$", desc, flags=re.I)
    if m:
        bus = m.group(1)
        desc = desc[:m.start()].strip(" ;")
    if desc.lower() in {"works", "", "?"}:
        desc = ""
    return desc, bus


def category(tm):
    t = (tm or "").lower()
    if "closure" in t:
        return "closure"
    if "one way" in t:
        return "oneway"
    if any(k in t for k in ("lanes reduced", "signals", "stop/go", "give/take")):
        return "restricted"
    return "minor"


def parse_dt(s):
    s = (s or "").strip()
    for fmt in ("%Y%m%d %H%M", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def clean(v):
    v = (str(v) if v is not None else "").strip()
    return "" if v.lower() in {"nan", "none", "?", "null"} else v


# ---------------------------------------------------------------- geometry
def path_length(p):
    return sum(math.dist(p[i], p[i + 1]) for i in range(len(p) - 1))


def point_along(p, frac=0.5):
    """Point at a fraction of the path's length (stays on the road, unlike a centroid)."""
    total = path_length(p)
    if total == 0:
        return p[0]
    target, run = total * frac, 0.0
    for i in range(len(p) - 1):
        seg = math.dist(p[i], p[i + 1])
        if run + seg >= target:
            t = (target - run) / seg if seg else 0
            return [p[i][0] + t * (p[i + 1][0] - p[i][0]), p[i][1] + t * (p[i + 1][1] - p[i][1])]
        run += seg
    return p[-1]


def r6(xy):
    return [round(xy[0], 6), round(xy[1], 6)]


def metres(a, b):
    """Approximate ground distance between two {lat,lng} dicts in Guernsey."""
    return math.hypot((a["lng"] - b["lng"]) * 72_500, (a["lat"] - b["lat"]) * 111_200)


MARKER_SPREAD_M = 700


def marker_points(roads):
    """One marker per job, plus one more per group of roads more than ~700 m
    away from any existing marker (storm call-outs, island-wide events).
    `roads` is sorted longest first, so the main road gets the first marker."""
    markers = []
    for r in roads:
        if all(metres(r, m) > MARKER_SPREAD_M for m in markers):
            markers.append({"lat": r["lat"], "lng": r["lng"]})
    return [[round(m["lat"], 6), round(m["lng"], 6)] for m in markers]


# ---------------------------------------------------------------- build
def build(seg_rows, proj_rows, today):
    tfm = Transformer.from_crs(SOURCE_CRS, "EPSG:4326", always_xy=True)
    projects = {clean(r["attributes"].get("PROJID")): r["attributes"] for r in proj_rows}
    window_end = today + timedelta(days=WINDOW_DAYS)

    jobs = defaultdict(list)
    dropped = Counter()
    for f in seg_rows:
        a = f.get("attributes") or {}
        paths = (f.get("geometry") or {}).get("paths") or []
        status = clean(a.get("STATUS"))
        tm = clean(a.get("Type"))
        if status not in KEEP_STATUS:
            dropped["status:" + status] += 1
            continue
        if tm.lower().startswith("diversion"):
            dropped["diversion"] += 1
            continue
        if not paths:
            dropped["no geometry"] += 1
            continue
        s, e = parse_dt(a.get("S_DATE")), parse_dt(a.get("E_DATE"))
        if not s or not e or e.date() < today or s.date() > window_end:
            dropped["outside window"] += 1
            continue
        ref = clean(a.get("NAME")) or clean(a.get("PROJID")) or f"JOB{a.get('JOBID')}"
        jobs[ref].append((a, [[list(tfm.transform(x, y)) for x, y in p] for p in paths]))
    print("dropped segments:", dict(dropped))

    features = []
    for ref, segs in jobs.items():
        attrs = [a for a, _ in segs]
        p = projects.get(ref, {})
        s = min(parse_dt(a["S_DATE"]) for a in attrs)
        e = max(parse_dt(a["E_DATE"]) for a in attrs)
        tm = clean(p.get("Type")) or Counter(clean(a.get("Type")) for a in attrs).most_common(1)[0][0]
        desc, bus_text = split_description(p.get("DESCRIPT") or "")
        public, school = parse_bus(bus_text)
        statuses = Counter(clean(a.get("STATUS")) for a in attrs)
        status = clean(p.get("Status")) or statuses.most_common(1)[0][0]
        if status == "Finished":
            status = statuses.most_common(1)[0][0]

        # Roads: one marker per road the job actually touches. Storm jobs list
        # dozens of roads under a placeholder project location.
        by_road = defaultdict(list)
        for a, paths in segs:
            road, parish = split_location(a.get("Location"))
            by_road[road].append((parish, paths))
        roads = []
        for road, items in by_road.items():
            paths = [x for _, ps in items for x in ps]
            longest = max(paths, key=path_length)
            lng, lat = point_along(longest)
            parish = Counter(pa for pa, _ in items).most_common(1)[0][0]
            roads.append({"road": road, "parish": parish, "lat": lat, "lng": lng,
                          "_len": sum(path_length(x) for x in paths)})
        roads.sort(key=lambda r: -r["_len"])
        markers = marker_points(roads)
        for r in roads:
            r.pop("_len")
            r["lat"], r["lng"] = round(r["lat"], 6), round(r["lng"], 6)

        # Project Location is usually "PARISH - ROAD"; for events it's a
        # mixed-case title ("Guernsey Rugby Club Firework Display"), and for
        # storm call-outs a placeholder car park.
        raw_loc = clean(p.get("Location") or attrs[0].get("Location"))
        main_road, main_parish = split_location(raw_loc)
        event = raw_loc.split(" - ", 1)[-1] if re.search(r"[a-z]", raw_loc.split(" - ", 1)[-1]) else ""
        if event:
            main_road = event.strip()
        elif main_road not in {r["road"] for r in roads}:
            main_road, main_parish = roads[0]["road"], roads[0]["parish"]
            if len(roads) > 1:
                main_road = f"{len(roads)} roads in {', '.join(sorted({r['parish'] for r in roads if r['parish']}))}"
                main_parish = ""
                # "Emergency Activity - Storm Austen - ..." -> "Storm Austen: 10 roads in ..."
                m = re.match(r"Emergency Activity\s*-\s*([^-]{3,60}?)\s+-\s", desc)
                if m:
                    main_road = f"{m.group(1).strip()}: {main_road}"

        lines = [[r6(xy) for xy in path] for _, paths in segs for path in paths]
        features.append({
            "type": "Feature",
            "geometry": {"type": "MultiLineString", "coordinates": lines},
            "properties": {
                "id": ref,
                "road": main_road,
                "parish": main_parish,
                "roads": roads,
                "markers": markers,
                "start": s.strftime("%Y-%m-%dT%H:%M"),
                "end": e.strftime("%Y-%m-%dT%H:%M"),
                "timing": clean(p.get("Timing")) or clean(attrs[0].get("Timing")),
                "tm_type": tm,
                "category": category(tm),
                "work_type": clean(p.get("WorkType")) or clean(attrs[0].get("WorkType")),
                "description": desc,
                "bus_routes": public,
                "school_buses": school,
                "promoter": clean(p.get("Promoter")) or clean(attrs[0].get("Promoter")),
                "contractor": clean(p.get("Contractor")),
                "status": status,
                "emergency": bool(re.search(r"\bemergency\b|\burgent\b", desc, re.I)) or clean(p.get("WorkType")) == "Police",
            },
        })

    features.sort(key=lambda f: (f["properties"]["start"], f["properties"]["id"]))
    return features


def sanity_check(features):
    if len(features) < 20:
        raise SystemExit(f"Only {len(features)} jobs built; refusing to overwrite {OUT}")
    lons = sorted(r["lng"] for f in features for r in f["properties"]["roads"])
    lats = sorted(r["lat"] for f in features for r in f["properties"]["roads"])
    mlon, mlat = lons[len(lons) // 2], lats[len(lats) // 2]
    if not (ISLAND_BOX["lon"][0] <= mlon <= ISLAND_BOX["lon"][1] and ISLAND_BOX["lat"][0] <= mlat <= ISLAND_BOX["lat"][1]):
        raise SystemExit(f"Median point {mlat},{mlon} is off-island; projection wrong, refusing to write")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixtures", help="read layer2.json / layer4.json from this folder instead of the API")
    ap.add_argument("--today", help="override today's date (YYYY-MM-DD), for tests")
    args = ap.parse_args()

    today = datetime.strptime(args.today, "%Y-%m-%d").date() if args.today else datetime.now(TZ).date()
    end = today + timedelta(days=WINDOW_DAYS)
    where = f"E_DATE >= '{today:%Y%m%d} 0000' AND S_DATE <= '{end:%Y%m%d} 2359'"

    if args.fixtures:
        d = Path(args.fixtures)
        segs = json.loads((d / "layer2.json").read_text())
        projs = json.loads((d / "layer4.json").read_text())
    else:
        segs = fetch_layer(LAYER_SEGMENTS, where, geometry=True)
        projs = fetch_layer(LAYER_PROJECTS, where, geometry=False)

    features = build(segs, projs, today)
    sanity_check(features)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    out = {
        "type": "FeatureCollection",
        "metadata": {
            "checked_at": now.isoformat().replace("+00:00", "Z"),
            "window_days": WINDOW_DAYS,
            "source": "States of Guernsey Traffic & Highways (roadworks.gov.gg)",
        },
        "features": features,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(OUT)
    cats = Counter(f["properties"]["category"] for f in features)
    print(f"Wrote {OUT}: {len(features)} jobs {dict(cats)} at {out['metadata']['checked_at']}")


if __name__ == "__main__":
    main()
