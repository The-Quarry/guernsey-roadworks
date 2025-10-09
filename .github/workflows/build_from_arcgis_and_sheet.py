#!/usr/bin/env python3
"""
Build enriched GeoJSON for Guernsey roadworks.

- Fetch native EsriJSON (no outSR), transform to WGS84 via pyproj from layer WKT / EPSG.
- Prefer EPSG:3108 (ETRS89 / Guernsey Grid) if WKID/WKT is unhelpful.
- Merge weekly spreadsheet fields: Road, Description, WorkType, BusRoutesAffected, Contractor.
- Join priority: 1) exact Ref match, 2) strict road name, 3) fuzzy road name; require date overlap.
- Filter to items overlapping the next WINDOW_DAYS (after enrichment).
- Write:
    data/closures.json         (Point centroids for markers)
    data/closures_lines.json   (Polyline geometry; popups wired too)
"""

import json, os, re, statistics, sys
from datetime import datetime, timedelta
from difflib import get_close_matches
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
from collections import defaultdict, Counter
from math import fsum
from html import unescape

import pandas as pd
from pyproj import CRS, Transformer

# -------------------- Config --------------------
SERVICE = "https://roadworks.gov.gg/arcgis/rest/services/GSW/MapServer"
LAYER = 2
WHERE_IDS = "1=1"
CHUNK = 200

WINDOW_DAYS = 90
SHEET = "latest.xlsx"      # or pass a path as argv[1]

OUT_POINTS = Path("data/closures.json")
OUT_LINES  = Path("data/closures_lines.json")

# Column mapping (matches your sheet)
COL = {
    "road":       "Roads",
    "start":      "Start Date",
    "end":        "End Date",
    "desc":       "Description",
    "what":       "WorkType",            # or "TM_Type"
    "routes":     "BusRoutesAffected",
    "contractor": "Contractor",          # or "Promoter"
    "ref":        "Ref",
    "tm_type":    "TM_Type",
    # sheet also has "Status" which we use to drop "Pending"
}

# Island ref/box for a safety check
GUERNSEY_REF = (-2.58, 49.455)
GUERNSEY_BOX = {"lon": (-2.7, -2.45), "lat": (49.40, 49.50)}
# ------------------------------------------------

# ---------------- HTTP / ArcGIS helpers ----------------
def _get_json(url: str, params: dict, timeout=60):
    q = url + "?" + urlencode(params)
    with urlopen(q, timeout=timeout) as r:
        return json.load(r)

def get_object_ids():
    data = _get_json(f"{SERVICE}/{LAYER}/query", {
        "where": WHERE_IDS,
        "returnIdsOnly": "true",
        "f": "json",
    })
    return data.get("objectIds") or data.get("objectIdArray") or []

def get_layer_crs() -> CRS:
    info = _get_json(f"{SERVICE}/{LAYER}", {"f": "pjson"})
    sr = (info.get("spatialReference")
          or info.get("extent", {}).get("spatialReference")
          or {})
    if sr.get("wkt"):
        try:
            crs = CRS.from_wkt(sr["wkt"])
            print("CRS: from WKT ->", crs.to_string())
            return crs
        except Exception as e:
            print("CRS: WKT parse failed:", e)
    wkid = sr.get("latestWkid") or sr.get("wkid")
    if wkid:
        try:
            crs = CRS.from_epsg(int(wkid))
            print(f"CRS: from WKID EPSG:{wkid} ->", crs.to_string())
            return crs
        except Exception as e:
            print(f"CRS: WKID {wkid} not resolvable:", e)
    print("CRS: fallback EPSG:3108 (ETRS89 / Guernsey Grid)")
    return CRS.from_epsg(3108)

def fetch_esri_native(ids):
    """Fetch features as EsriJSON IN NATIVE CRS (no outSR)."""
    feats = []
    url = f"{SERVICE}/{LAYER}/query"
    total = len(ids)
    for i in range(0, total, CHUNK):
        subset = ids[i:i+CHUNK]
        page = _get_json(url, {
            "objectIds": ",".join(map(str, subset)),
            "outFields": "*",
            "returnGeometry": "true",
            "f": "json",  # EsriJSON (native CRS)
        })
        feats.extend(page.get("features", []))
        print(f"ArcGIS batch {i+len(subset):5d}/{total}   (+{len(page.get('features',[]))})")
    return feats
# -------------------------------------------------------

# ---------------- Geometry helpers ----------------
def transform_paths(paths, tfm: Transformer):
    out = []
    for line in (paths or []):
        out.append([list(tfm.transform(x, y)) for x, y in line])
    return out

def lines_from_paths(paths_ll):
    if not paths_ll:
        return None
    if len(paths_ll) == 1:
        return {"type": "LineString", "coordinates": paths_ll[0]}
    return {"type": "MultiLineString", "coordinates": paths_ll}

def centroid_of_lines(geom):
    if not geom:
        return (None, None)
    if geom["type"] == "LineString":
        coords = geom["coordinates"]
    else:
        coords = [pt for line in geom["coordinates"] for pt in line]
    if not coords:
        return (None, None)
    xs = [x for x, _ in coords]
    ys = [y for _, y in coords]
    return (statistics.mean(xs), statistics.mean(ys))
# --------------------------------------------------

# ---------------- Dates & merge helpers ----------------
DATE_FORMATS = [
    "%d/%m/%Y",      # 23/09/2025
    "%Y-%m-%d",      # 2025-09-23
    "%m/%d/%Y",      # 09/23/2025
    "%Y%m%d",        # 20250923
    "%Y%m%d %H%M",   # 20250923 0830
]

def parse_date(v):
    if v is None or str(v).strip() == "":
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.date()
    s = str(v).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(s, dayfirst=True, errors="coerce").date()
    except Exception:
        return None

def overlap(a_start, a_end, b_start, b_end):
    if not a_start and not a_end:
        return True
    if not b_start and not b_end:
        return True
    if not a_start: a_start = a_end
    if not a_end:   a_end   = a_start
    if not b_start: b_start = b_end
    if not b_end:   b_end   = b_start
    if not a_start or not a_end or not b_start or not b_end:
        return True
    return not (a_end < b_start or a_start > b_end)

def norm_road(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()

def normalize_routes(s):
    """
    Clean and split the spreadsheet's BusRoutesAffected column.

    - HTML-decode (&quot;, &amp;, etc.)
    - strip straight/smart quotes and other stray punctuation
    - split on common delimiters ; , / | and whitespace
    - normalize case (e.g. 2a -> 2A when there's a trailing letter)
    - drop obvious HTML-artifact tokens like 'quot'
    """
    if pd.isna(s):
        return []

    # 1) HTML-decode first (turn &quot; into ")
    txt = unescape(str(s))

    # 2) unify smart quotes → straight, then strip quotes
    txt = (txt.replace("“", '"').replace("”", '"').replace("’", "'"))

    # 3) split into tokens
    tokens = re.split(r'[;,/|+\s]+', txt)

    out = []
    for t in tokens:
        t = t.strip()
        if not t:
            continue

        # remove any leftover quotes
        t = t.strip('\'"')

        # remove non-alphanumerics except hyphen (keep things like 91-92)
        t = re.sub(r'[^0-9A-Za-z\-]', '', t)

        # drop pure HTML artifacts
        if t.lower() in {'quot', 'amp', 'nbsp'} or not t:
            continue

        # normalize 2a -> 2A (digits with optional trailing letter)
        if re.fullmatch(r'[0-9]+[A-Za-z]?', t):
            if t[-1:].isalpha():
                t = t[:-1] + t[-1:].upper()

        out.append(t)

    return out

def tidy_tm(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    t = s.title().replace("Am", "AM").replace("Pm", "PM")
    return t
# -------------------------------------------------------

# ---------------- Diversion detector ----------------
DIVERSION_PAT = re.compile(r"\bdiversion\b|\bdivert(?:ed|ing)?\b", re.I)
DIVERSION_HINT_KEYS = [
    "Type", "TYPE", "Restriction", "RESTRICTION",
    "TM_Type", "TMType", "DESCRIPT", "DESCRIPTION",
    "NAME", "Location", "DETAILS", "Notes"
]

def looks_like_diversion(attrs: dict, merged_props: dict | None = None) -> bool:
    """Return True if attributes or merged props suggest this is a diversion route."""
    for k in DIVERSION_HINT_KEYS:
        v = attrs.get(k)
        if v and DIVERSION_PAT.search(str(v)):
            return True
    txt = " ".join(str(attrs.get(k) or "") for k in ("DESCRIPT", "DESCRIPTION", "Notes"))
    if DIVERSION_PAT.search(txt):
        return True
    if merged_props:
        for k in ("tm_type", "what", "description", "road"):
            v = merged_props.get(k) or ""
            if v and DIVERSION_PAT.search(str(v)):
                return True
    return False
# -------------------------------------------------------

# ---------------- Alignment helpers ----------------
def median_centroid(features):
    xs, ys = [], []
    for f in features:
        g = f["geometry"]
        if g["type"] == "Point":
            x, y = g["coordinates"]; xs.append(x); ys.append(y)
        elif g["type"] == "LineString":
            xs += [x for x, y in g["coordinates"]]
            ys += [y for x, y in g["coordinates"]]
        elif g["type"] == "MultiLineString":
            for line in g["coordinates"]:
                xs += [x for x, y in line]
                ys += [y for x, y in line]
    if not xs:
        return None
    xs.sort(); ys.sort()
    mid = len(xs)//2
    cx = xs[mid] if len(xs) % 2 else (xs[mid-1]+xs[mid])/2
    cy = ys[mid] if len(ys) % 2 else (ys[mid-1]+ys[mid])/2
    return (cx, cy)

def apply_delta(feature, dx, dy):
    g = feature["geometry"]
    if g["type"] == "Point":
        x, y = g["coordinates"]; g["coordinates"] = [x + dx, y + dy]
    elif g["type"] == "LineString":
        g["coordinates"] = [[x + dx, y + dy] for x, y in g["coordinates"]]
    elif g["type"] == "MultiLineString":
        g["coordinates"] = [[[x + dx, y + dy] for x, y in line] for line in g["coordinates"]]
# --------------------------------------------------

def _key_for_job(p):
    """Prefer ref for grouping; fallback to (road,start,end)."""
    ref = (p.get("reference") or "").strip()
    if ref:
        return ("REF", ref)
    return ("ROADDATES",
            (p.get("road") or "").strip().lower(),
            p.get("start_date") or "",
            p.get("end_date") or "")

def _merge_props(props_list):
    """Combine multiple segment props of the same job into one."""
    if not props_list:
        return {}
    starts = [p.get("start_date") for p in props_list if p.get("start_date")]
    ends   = [p.get("end_date")   for p in props_list if p.get("end_date")]
    descs = sorted((p.get("description") or "" for p in props_list), key=len, reverse=True)
    whats = [p.get("what") or "" for p in props_list]
    roads = [p.get("road") or "" for p in props_list]
    contr = next((p.get("contractor") for p in props_list if p.get("contractor")), "")
    status= next((p.get("status") for p in props_list if p.get("status")), "")
    ref   = next((p.get("reference") for p in props_list if p.get("reference")), "")
    tms   = [(p.get("tm_type") or "").strip() for p in props_list if (p.get("tm_type") or "").strip()]
    tm_type = tidy_tm(Counter(tms).most_common(1)[0][0]) if tms else ""
    routes = []
    for p in props_list:
        routes += (p.get("bus_routes") or [])
    routes = sorted({r.strip() for r in routes if r and r.strip()},
                    key=lambda x: (x.isdigit(), x))
    return {
        "road": max(roads, key=len) if roads else "",
        "start_date": min(starts) if starts else None,
        "end_date":   max(ends)   if ends   else None,
        "what": next((w for w in whats if w), "") or (whats[0] if whats else ""),
        "description": (descs[0] if descs else ""),
        "bus_routes": routes,
        "contractor": contr,
        "reference": ref,
        "status": status,
        "tm_type": tm_type,
        "source": "merged",
    }

def _merge_points_and_lines(out_pts, out_lines):
    groups = defaultdict(lambda: {"props": [], "pts": [], "paths": []})
    for pt in out_pts:
        p = pt.get("properties", {})
        key = _key_for_job(p)
        if pt.get("geometry", {}).get("type") == "Point":
            lng, lat = pt["geometry"]["coordinates"]
            groups[key]["pts"].append((lng, lat))
        groups[key]["props"].append(p)
    for ln in out_lines:
        p = ln.get("properties", {})
        key = _key_for_job(p)
        g = ln.get("geometry", {})
        if not g: continue
        if g["type"] == "LineString":
            groups[key]["paths"].append(g["coordinates"])
        elif g["type"] == "MultiLineString":
            groups[key]["paths"].extend(g["coordinates"])
        groups[key]["props"].append(p)

    merged_pts, merged_lines = [], []
    for key, bag in groups.items():
        props = _merge_props(bag["props"])
        if bag["paths"]:
            geom_lines = {"type": "MultiLineString", "coordinates": bag["paths"]}
        else:
            geom_lines = None
        if bag["pts"]:
            lngs = [lng for lng, _ in bag["pts"]]
            lats = [lat for _, lat in bag["pts"]]
            clng = fsum(lngs) / len(lngs)
            clat = fsum(lats) / len(lats)
        else:
            clng = clat = None
            if geom_lines:
                flat = [xy for line in geom_lines["coordinates"] for xy in line]
                if flat:
                    xs = [x for x, _ in flat]; ys = [y for _, y in flat]
                    clng = fsum(xs)/len(xs); clat = fsum(ys)/len(ys)
        if clng is not None and clat is not None:
            merged_pts.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [clng, clat]},
                "properties": props
            })
        if geom_lines:
            merged_lines.append({
                "type": "Feature",
                "geometry": geom_lines if len(geom_lines["coordinates"])>1
                            else {"type":"LineString","coordinates":geom_lines["coordinates"][0]},
                "properties": props
            })
    return merged_pts, merged_lines

def main():
    os.makedirs("data", exist_ok=True)
    today = datetime.today().date()
    window_end = today + timedelta(days=WINDOW_DAYS)

    # IDs & CRS
    ids = get_object_ids()
    if not ids:
        print("No object IDs returned — check WHERE_IDS or layer availability.")
        sys.exit(1)
    print(f"Found {len(ids)} object IDs")

    src_crs = get_layer_crs()
    dst_crs = CRS.from_epsg(4326)   # WGS84 lon/lat
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    # Fetch native & transform
    esri_feats = fetch_esri_native(ids)
    print(f"Fetched {len(esri_feats)} raw features")

    # Load spreadsheet (drop Pending rows up-front if column exists)
    sheet_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(SHEET)
    df = None
    if sheet_path.exists():
        df = (pd.read_excel(sheet_path)
              if sheet_path.suffix.lower() in (".xlsx", ".xls")
              else pd.read_csv(sheet_path))
        for k, v in COL.items():
            if v not in df.columns:
                print(f"Warning: spreadsheet column {v!r} not found.")
        # Drop "Pending" (case-insensitive)
        if "Status" in df.columns:
            df = df[~df["Status"].astype(str).str.strip().str.lower().eq("pending")].copy()
        elif "STATUS" in df.columns:
            df = df[~df["STATUS"].astype(str).str.strip().str.lower().eq("pending")].copy()

        df["_road_norm"] = df.get(COL["road"], "").astype(str).map(norm_road)
        if COL["start"] in df.columns: df["_start"] = df[COL["start"]].map(parse_date)
        if COL["end"]   in df.columns: df["_end"]   = df[COL["end"]].map(parse_date)
    else:
        print(f"Spreadsheet not found at {sheet_path} — proceeding with ArcGIS attributes only.")

    out_pts, out_lines = [], []

    # Transform, enrich
    for f in esri_feats:
        attrs = f.get("attributes", {}) or {}
        paths = (f.get("geometry") or {}).get("paths") or []
        if not paths:
            continue

        # transform to WGS84
        wgs_paths = transform_paths(paths, transformer)
        lines = lines_from_paths(wgs_paths)
        lon, lat = centroid_of_lines(lines)

        # ArcGIS attributes (capture ref & road & raw dates)
        ref_arc   = (attrs.get("NAME") or attrs.get("Ref") or attrs.get("JOBID") or "").strip()
        road_arc  = (attrs.get("Location") or attrs.get("ROAD") or attrs.get("NAME") or "").strip()
        start_arc = parse_date(attrs.get("S_DATE") or attrs.get("Start") or attrs.get("StartDate"))
        end_arc   = parse_date(attrs.get("E_DATE") or attrs.get("End")   or attrs.get("EndDate"))

        props = {
            "road":        road_arc,
            "start_date":  start_arc.isoformat() if start_arc else None,
            "end_date":    end_arc.isoformat()   if end_arc   else None,
            "what":        (attrs.get("WorkType") or attrs.get("Type") or "").strip(),
            "description": (attrs.get("DESCRIPT") or attrs.get("DESCRIPTION") or "").strip(),
            "bus_routes":  [],
            "contractor":  (attrs.get("Promoter") or attrs.get("CONTRACTOR") or "").strip(),
            "reference":   ref_arc,   # retained only for joins/debug
            "status":      (attrs.get("STATUS") or attrs.get("Status") or "").strip(),
            "tm_type":     "",
            "source":      f"GSW/MapServer/{LAYER}",
        }

        # --- Enrich from spreadsheet: Ref -> road strict -> road fuzzy ---
        chosen = None
        if df is not None and len(df):
            # (1) exact Ref
            if COL.get("ref") in df.columns and ref_arc:
                cand = df[df[COL["ref"]].astype(str).str.strip() == ref_arc]
                if not cand.empty:
                    for _, r in cand.iterrows():
                        if overlap(start_arc, end_arc, r.get("_start"), r.get("_end")):
                            chosen = r; break
                    if chosen is None:
                        chosen = cand.iloc[0]
            # (2) strict road name
            if chosen is None:
                rn = norm_road(road_arc)
                if rn:
                    cand = df[df["_road_norm"] == rn]
                    if not cand.empty:
                        for _, r in cand.iterrows():
                            if overlap(start_arc, end_arc, r.get("_start"), r.get("_end")):
                                chosen = r; break
                        if chosen is None:
                            chosen = cand.iloc[0]
            # (3) fuzzy road
            if chosen is None:
                rn = norm_road(road_arc)
                if rn:
                    guess = get_close_matches(rn, df["_road_norm"].dropna().unique().tolist(), n=1, cutoff=0.93)
                    if guess:
                        cand = df[df["_road_norm"] == guess[0]]
                        if not cand.empty:
                            chosen = cand.iloc[0]

        if chosen is not None:
            def g(col): return str(chosen.get(col) or "").strip()
            rd = g(COL["road"])
            if rd: props["road"] = rd
            props["description"] = g(COL["desc"]) or props["description"]
            props["what"]        = g(COL["what"]) or props["what"]
            props["contractor"]  = g(COL["contractor"]) or props["contractor"]
            props["bus_routes"]  = normalize_routes(chosen.get(COL["routes"]))
            props["tm_type"]     = tidy_tm(g(COL.get("tm_type"))) or props.get("tm_type") or ""
            # Sheet dates win if present
            s_sheet = chosen.get("_start")
            e_sheet = chosen.get("_end")
            if s_sheet: props["start_date"] = s_sheet.isoformat()
            if e_sheet: props["end_date"]   = e_sheet.isoformat()
            # Carry sheet Status if present (already removed 'Pending' rows above)
            status_sheet = str(chosen.get("Status") or chosen.get("STATUS") or "").strip()
            if status_sheet:
                props["status"] = status_sheet

        # Final window filter AFTER enrichment
        s_eff = parse_date(props["start_date"])
        e_eff = parse_date(props["end_date"])
        if not overlap(s_eff, e_eff, today, today + timedelta(days=WINDOW_DAYS)):
            continue

        # Skip diversion geometries entirely
        if looks_like_diversion(attrs, props):
            # print("Skip diversion:", props.get("road"), props.get("start_date"), props.get("end_date"))
            continue

        # outputs
        if lon is not None and lat is not None:
            out_pts.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props
            })
        out_lines.append({
            "type": "Feature",
            "geometry": lines,
            "properties": props
        })

    # Safety: if everything off-island, auto-nudge once
    check = out_lines if out_lines else out_pts
    cen = median_centroid(check) if check else None
    if cen:
        cx, cy = cen
        in_box = (GUERNSEY_BOX["lon"][0] <= cx <= GUERNSEY_BOX["lon"][1] and
                  GUERNSEY_BOX["lat"][0] <= cy <= GUERNSEY_BOX["lat"][1])
        if not in_box:
            dx = GUERNSEY_REF[0] - cx
            dy = GUERNSEY_REF[1] - cy
            for f in out_pts:   apply_delta(f, dx, dy)
            for f in out_lines: apply_delta(f, dx, dy)
            print(f"Applied alignment Δlon={dx:+.6f}, Δlat={dy:+.6f}")

    # --- DEDUPE / MERGE by job ---
    merged_pts, merged_lines = _merge_points_and_lines(out_pts, out_lines)
    print(f"Merging: {len(out_pts)}→{len(merged_pts)} points, {len(out_lines)}→{len(merged_lines)} line features")

    # Write files (with build metadata)
    built_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    OUT_POINTS.parent.mkdir(parents=True, exist_ok=True)
    with OUT_POINTS.open("w", encoding="utf-8") as f:
        json.dump({"type":"FeatureCollection","features":merged_pts,"metadata":{"built_at":built_at}}, f, ensure_ascii=False, indent=2)
    with OUT_LINES.open("w", encoding="utf-8") as f:
        json.dump({"type":"FeatureCollection","features":merged_lines,"metadata":{"built_at":built_at}}, f, ensure_ascii=False, indent=2)

    print(f"Built at {built_at}")
    print(f"Wrote {OUT_POINTS} with {len(merged_pts)} features")
    print(f"Wrote {OUT_LINES} with {len(merged_lines)} features")

if __name__ == "__main__":
    main()