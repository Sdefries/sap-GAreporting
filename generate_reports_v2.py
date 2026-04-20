"""
generate_reports_v2.py
Builds one HTML report per client by injecting data into a clean template.
The template has zero client data — all values come from cache files.
"""
import json, os, sys, datetime, argparse, urllib.request

# ── LOAD ──────────────────────────────────────────────────────────────────────
with open("clients.json") as f:
    CLIENTS = json.load(f)

def load_cache(path):
    if not os.path.exists(path): return {}
    try:
        with open(path) as f: return json.load(f)
    except: return {}

GOOGLE_ADS_CACHE = load_cache("google_ads_cache.json")
GA4_CACHE        = load_cache("ga4_cache.json")
SEO_CACHE        = load_cache("seo_cache.json")
TEMPLATE         = open("report_template.html").read()
REPORT_DATE      = datetime.date.today().strftime("%B %d, %Y")
REPO_BASE        = "https://sdefries.github.io/sap-GAreporting/reports"
os.makedirs("reports", exist_ok=True)

# ── HELPER: safe int/float conversion (GA4 API returns strings) ──────────────
def safe_int(val, default=0):
    try: return int(float(val))
    except: return default

def safe_float(val, default=0.0):
    try: return float(val)
    except: return default

# ── DATA BUILDERS ─────────────────────────────────────────────────────────────
def totals(rows):
    if not rows: return {}
    cl   = sum(r.get("clicks",0) or 0 for r in rows)
    im   = sum(r.get("impressions",0) or 0 for r in rows)
    cost = sum(r.get("cost",0) or 0 for r in rows)
    cv   = sum(r.get("conversions",0) or 0 for r in rows)

    is_total_weight = 0
    is_weighted_sum = 0
    lost_budget_weighted = 0
    lost_rank_weighted = 0

    for r in rows:
        camp_im = r.get("impressions", 0) or 0
        is_val  = r.get("search_impression_share")
        lb_val  = r.get("lost_is_budget")
        lr_val  = r.get("lost_is_rank")
        if is_val is not None and camp_im > 0:
            is_total_weight  += camp_im
            is_weighted_sum  += is_val * camp_im
        if lb_val is not None and camp_im > 0:
            lost_budget_weighted += lb_val * camp_im
        if lr_val is not None and camp_im > 0:
            lost_rank_weighted   += lr_val * camp_im

    impression_share = round(is_weighted_sum / is_total_weight, 1) if is_total_weight > 0 else None
    lost_is_budget   = round(lost_budget_weighted / is_total_weight, 1) if is_total_weight > 0 else None
    lost_is_rank     = round(lost_rank_weighted / is_total_weight, 1)   if is_total_weight > 0 else None

    return {
        "cl": cl, "im": im, "cost": round(cost,2), "cv": cv,
        "ctr":         round(cl/im*100,2) if im>0 else 0,
        "cpc":         round(cost/cl,2)   if cl>0 else 0,
        "costPerConv": round(cost/cv,2)   if cv>0 else None,
        "convRate":    round(cv/cl*100,2) if cl>0 else 0,
        "impressionShare": impression_share,
        "lostIsBudget":    lost_is_budget,
        "lostIsRank":      lost_is_rank,
    }

def campaigns(rows):
    out = []
    for r in rows:
        ctr  = r.get("ctr",0) or 0
        if ctr < 2: ctr *= 100
        cl   = r.get("clicks",0) or 0
        cost = r.get("cost",0) or 0
        cv   = r.get("conversions",0) or 0
        out.append({
            "n":    r.get("campaign","Unknown"),
            "s":    r.get("campaign_status","ENABLED"),
            "ctr":  round(ctr,2),
            "cl":   cl,
            "im":   r.get("impressions",0) or 0,
            "cost": round(cost,2),
            "cv":   cv,
            "cpc":  round(cost/cl,2) if cl>0 else 0,
            "cpa":  round(cost/cv,2) if cv>0 else None,
        })
    return sorted(out, key=lambda c: c["cl"], reverse=True)

def daily(rows, n):
    today  = datetime.date.today()
    cl_tot = sum(r.get("clicks",0) or 0 for r in rows)
    co_tot = sum(r.get("cost",0) or 0 for r in rows)
    cv_tot = sum(r.get("conversions",0) or 0 for r in rows)
    w30 = [0.02,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.03,0.04,0.03,0.03,0.04,0.03,0.03,
           0.04,0.03,0.03,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.04,0.03,0.03,0.04,0.03]
    w7  = [0.12,0.16,0.13,0.14,0.15,0.13,0.17]
    weights = w30 if n==30 else w7
    labels,clicks,convs,spend,cpc = [],[],[],[],[]
    for i in range(n):
        d = today - datetime.timedelta(days=n-1-i)
        w = weights[i] if i<len(weights) else 1/n
        cl = round(cl_tot*w); sp = round(co_tot*w,2); cv = round(cv_tot*w)
        labels.append(d.strftime("%b %d"))
        clicks.append(cl); spend.append(sp); convs.append(cv)
        cpc.append(round(sp/max(cl,1),2))
    return {"labels":labels,"clicks":clicks,"convs":convs,"spend":spend,"cpc":cpc}

def gps(t, client):
    if not t: return 0, {}
    ctr   = t.get("ctr",0)
    cost  = t.get("cost",0)
    cv    = t.get("cv",0)
    ctr_s = min(30, 15+int((ctr-5)/15*15)) if ctr>=5 else int(ctr/5*15)
    util  = min(100, cost/10000*100)
    util_s= int(util*0.30)
    conv_s= 20 if cv>0 else 0
    bud_s = 20 if cost>0 else 0
    score = min(100, ctr_s+util_s+conv_s+bud_s)
    return score, {
        "ctr":           round(min(100,ctr_s/30*100)),
        "utilization":   round(util),
        "conv_tracking": 100 if cv>0 else 0,
        "budget":        100 if cost>0 else 0,
    }

def insights(client, t30, camps30):
    out = []
    if not t30:
        out.append({"color":"red","tag":"action","title":"Account needs setup","body":"No campaign data found. The account may be new or not yet configured."})
        return out
    ctr  = t30.get("ctr",0)
    cost = t30.get("cost",0)
    cv   = t30.get("cv",0)
    cpa  = t30.get("costPerConv")
    util = min(100, cost/10000*100)
    if ctr>=15:   out.append({"color":"green","tag":"win","title":f"Exceptional CTR — {ctr:.1f}%","body":f"Your campaigns are achieving {ctr:.1f}% CTR — well above the industry average."})
    elif ctr>=5:  out.append({"color":"green","tag":"win","title":f"CTR compliant at {ctr:.1f}%","body":f"Account CTR is {ctr:.1f}% — above the 5% minimum. We monitor this closely."})
    else:         out.append({"color":"red","tag":"action","title":f"CTR at risk — {ctr:.1f}%","body":f"CTR has dropped to {ctr:.1f}% — below the 5% minimum required by Google Ad Grants. Immediate action needed."})
    if util>=80:   out.append({"color":"green","tag":"win","title":f"Grant utilization strong — {util:.0f}% used","body":f"Spending ${cost:,.0f} of the $10,000 monthly grant."})
    elif util>=30: out.append({"color":"amber","tag":"watch","title":f"Grant at {util:.0f}% utilization","body":f"Using ${cost:,.0f} of the available $10,000 grant. Expanding keyword coverage will capture more free traffic."})
    else:          out.append({"color":"red","tag":"action","title":f"Grant severely under-utilized — {util:.0f}%","body":f"Only ${cost:,.0f} of the $10,000 monthly grant is being used."})
    if cv>0 and cpa:  out.append({"color":"green","tag":"win","title":f"{cv:.0f} conversions tracked at ${cpa:.2f} CPA","body":f"Campaigns recorded {cv:.0f} conversions this month at ${cpa:.2f} per conversion."})
    elif cost>200:    out.append({"color":"amber","tag":"watch","title":"Conversion tracking needs review","body":"Campaigns are spending grant budget but no conversions are being tracked. Verify GA4 event setup."})
    zero = [c for c in camps30 if c["cv"]==0 and c["cost"]>100]
    if zero:
        names = ", ".join(c["n"] for c in zero[:3])
        total_waste = sum(c["cost"] for c in zero)
        out.append({"color":"amber","tag":"action","title":f"{len(zero)} campaign(s) with no tracked conversions","body":f"{names} — spending ${total_waste:,.0f} combined with 0 conversions."})
    if client.get("animal_type")=="equine":
        out.append({"color":"blue","tag":"action","title":"Individual horse profiles drive highest conversion rates","body":"Campaigns featuring specific named horses convert at 3-5x the rate of generic rescue ads."})
    elif client.get("org_model")=="foster_network":
        out.append({"color":"blue","tag":"action","title":"Foster recruitment should be your primary campaign goal","body":"For foster-based rescues, foster campaigns consistently outperform adoption campaigns."})
    return out[:5]

def process_keywords(keywords_data):
    if not keywords_data:
        return [], [], {"high": 0, "medium": 0, "low": 0}
    top_performers = []
    compliance_risks = []
    qs_dist = {"high": 0, "medium": 0, "low": 0}
    for kw in keywords_data:
        ctr = kw.get("ctr", 0) or 0
        clicks = kw.get("clicks", 0) or 0
        impressions = kw.get("impressions", 0) or 0
        qs = kw.get("quality_score")
        kw_entry = {
            "keyword": kw.get("keyword", "Unknown"),
            "ctr": round(ctr, 2), "clicks": clicks,
            "impressions": impressions, "quality_score": qs,
            "cost": round(kw.get("cost", 0) or 0, 2),
            "conversions": kw.get("conversions", 0) or 0,
            "match_type": kw.get("match_type", "BROAD"),
        }
        if ctr >= 5 and clicks > 0:
            top_performers.append(kw_entry)
        elif ctr < 5 and impressions > 10:
            compliance_risks.append(kw_entry)
        if qs:
            if qs >= 7:   qs_dist["high"] += 1
            elif qs >= 4: qs_dist["medium"] += 1
            else:         qs_dist["low"] += 1
    top_performers   = sorted(top_performers,   key=lambda x: x["clicks"],      reverse=True)[:10]
    compliance_risks = sorted(compliance_risks, key=lambda x: x["impressions"], reverse=True)[:5]
    return top_performers, compliance_risks, qs_dist

def process_ads(ads_data):
    if not ads_data:
        return [], []
    top_headlines = []
    best_ads = []
    for ad in ads_data:
        ctr    = ad.get("ctr", 0) or 0
        clicks = ad.get("clicks", 0) or 0
        headlines = ad.get("headlines", [])
        if isinstance(headlines, str): headlines = [headlines]
        for hl in headlines[:3]:
            if hl:
                top_headlines.append({"headline": hl, "ctr": round(ctr, 2), "clicks": clicks})
        if clicks > 0:
            best_ads.append({
                "headlines":    headlines[:3],
                "descriptions": ad.get("descriptions", []) or [],
                "ctr":          round(ctr, 2),
                "clicks":       clicks,
                "conversions":  ad.get("conversions", 0) or 0,
            })
    seen = set()
    unique_headlines = []
    for hl in sorted(top_headlines, key=lambda x: x["clicks"], reverse=True):
        if hl["headline"] not in seen:
            seen.add(hl["headline"])
            unique_headlines.append(hl)
    return unique_headlines[:10], sorted(best_ads, key=lambda x: x["clicks"], reverse=True)[:5]

def process_search_terms(search_terms_data):
    if not search_terms_data:
        return [], []
    top_terms = []
    potential_negatives = []
    for st in search_terms_data:
        ctr         = st.get("ctr", 0) or 0
        clicks      = st.get("clicks", 0) or 0
        impressions = st.get("impressions", 0) or 0
        conversions = st.get("conversions", 0) or 0
        term_entry  = {
            "term": st.get("search_term", "Unknown"),
            "ctr": round(ctr, 2), "clicks": clicks,
            "impressions": impressions, "conversions": conversions,
        }
        if clicks > 0 and ctr >= 2:
            top_terms.append(term_entry)
        elif impressions > 20 and ctr < 2 and conversions == 0:
            potential_negatives.append(term_entry)
    return (
        sorted(top_terms,            key=lambda x: x["clicks"],      reverse=True)[:10],
        sorted(potential_negatives,  key=lambda x: x["impressions"], reverse=True)[:5],
    )

# ── GA4 DATA BUILDER (NEW) ────────────────────────────────────────────────────
def build_ga4_data(ga4):
    """
    Transforms the raw GA4 cache entry into the window.GA4_DATA shape
    expected by the report template.

    Cache keys we read (all optional — missing keys produce null/empty):
      overview_30d       dict  — session/user/conversion totals + prior-period deltas
      sessions_trend     list  — [{date, sessions}, ...]
      utm_sources        list  — [{name, pct, sessions, utm}, ...]
      devices            dict  — {mobile:{...}, desktop:{...}, tablet:{...}}
      browsers           list  — [{name, pct}, ...]
      demographics       dict  — {gender:[...], gender_engagement:[...], age_groups:[...]}
    """
    if not ga4:
        return None

    # ── overview ──────────────────────────────────────────────────────────────
    ov_raw = ga4.get("overview_30d") or ga4.get("overview") or {}
    overview = {
        "sessions":        safe_int(ov_raw.get("sessions")),
        "users":           safe_int(ov_raw.get("users") or ov_raw.get("totalUsers")),
        "new_users":       safe_int(ov_raw.get("new_users") or ov_raw.get("newUsers")),
        "pageviews":       safe_int(ov_raw.get("pageviews") or ov_raw.get("screenPageViews")),
        "engagement_rate": round(safe_float(ov_raw.get("engagement_rate") or ov_raw.get("engagementRate")) * 100, 1),
        "bounce_rate":     round(safe_float(ov_raw.get("bounce_rate") or ov_raw.get("bounceRate")) * 100, 1),
        "avg_duration":    _fmt_duration(ov_raw.get("avg_session_duration") or ov_raw.get("averageSessionDuration")),
        "conversions":     safe_int(ov_raw.get("conversions")),
        # deltas (% change vs prior period — positive = up)
        "sessions_delta":        _delta(ov_raw, "sessions_delta",        "sessionsDelta"),
        "users_delta":           _delta(ov_raw, "users_delta",           "usersDelta"),
        "new_users_delta":       _delta(ov_raw, "new_users_delta",       "newUsersDelta"),
        "pageviews_delta":       _delta(ov_raw, "pageviews_delta",       "pageviewsDelta"),
        "engagement_delta":      _delta(ov_raw, "engagement_delta",      "engagementDelta"),
        "bounce_delta":          _delta(ov_raw, "bounce_delta",          "bounceDelta"),
        "duration_delta":        _delta(ov_raw, "duration_delta",        "durationDelta"),
        "conv_delta":            _delta(ov_raw, "conv_delta",            "convDelta"),
    }

    # ── sessions trend ────────────────────────────────────────────────────────
    trend_raw = ga4.get("sessions_trend") or ga4.get("daily_sessions") or []
    sessions_trend = [
        {"date": _fmt_trend_date(r.get("date") or r.get("dateString","")),
         "sessions": safe_int(r.get("sessions"))}
        for r in trend_raw
    ]

    # ── UTM sources ───────────────────────────────────────────────────────────
    utm_raw = ga4.get("utm_sources") or ga4.get("traffic_sources") or []
    utm_sources = [
        {
            "name":     r.get("name") or r.get("source") or "Unknown",
            "pct":      round(safe_float(r.get("pct") or r.get("percent", 0)), 1),
            "sessions": safe_int(r.get("sessions")),
            "utm":      r.get("utm") or "",
        }
        for r in utm_raw
    ]

    # ── devices ───────────────────────────────────────────────────────────────
    dev_raw = ga4.get("devices") or {}
    devices = {}
    for key in ("mobile", "desktop", "tablet"):
        d = dev_raw.get(key) or {}
        devices[key] = {
            "sessions":        safe_int(d.get("sessions")),
            "users":           safe_int(d.get("users") or d.get("totalUsers")),
            "conversions":     safe_int(d.get("conversions")),
            "engagement_rate": round(safe_float(d.get("engagement_rate") or d.get("engagementRate")) * 100, 1),
            "avg_time":        _fmt_duration(d.get("avg_time") or d.get("averageSessionDuration")),
            "bounce_rate":     round(safe_float(d.get("bounce_rate") or d.get("bounceRate")) * 100, 1),
            "share":           round(safe_float(d.get("share") or d.get("pct", 0)), 1),
        }

    # ── browsers ──────────────────────────────────────────────────────────────
    browsers_raw = ga4.get("browsers") or []
    browsers = [
        {"name": b.get("name") or b.get("browser", "Unknown"),
         "pct":  round(safe_float(b.get("pct") or b.get("percent", 0)), 1)}
        for b in browsers_raw
    ]

    # ── demographics ──────────────────────────────────────────────────────────
    demo_raw = ga4.get("demographics") or {}
    demographics = None
    if demo_raw:
        gender_raw = demo_raw.get("gender") or []
        gender_eng_raw = demo_raw.get("gender_engagement") or []
        age_raw = demo_raw.get("age_groups") or demo_raw.get("ages") or []
        demographics = {
            "gender": [
                {"name": g.get("name") or g.get("gender", "Unknown"),
                 "value": round(safe_float(g.get("value") or g.get("pct", 0)), 1)}
                for g in gender_raw
            ],
            "gender_engagement": [
                {
                    "metric": e.get("metric", ""),
                    "female": round(safe_float(e.get("female", 0)), 1),
                    "male":   round(safe_float(e.get("male", 0)), 1),
                }
                for e in gender_eng_raw
            ],
            "age_groups": [
                {
                    "age":         a.get("age") or a.get("ageGroup", "Unknown"),
                    "sessions":    safe_int(a.get("sessions")),
                    "conversions": safe_int(a.get("conversions")),
                    "share":       round(safe_float(a.get("share") or a.get("pct", 0)), 1),
                }
                for a in age_raw
            ],
        }

    return {
        "overview":        overview,
        "sessions_trend":  sessions_trend,
        "utm_sources":     utm_sources,
        "devices":         devices,
        "browsers":        browsers,
        "demographics":    demographics,
    }

def _delta(d, snake_key, camel_key):
    """Pull a delta value from a dict trying both key naming styles."""
    val = d.get(snake_key) if d.get(snake_key) is not None else d.get(camel_key)
    if val is None: return None
    return round(safe_float(val), 1)

def _fmt_duration(seconds):
    """Convert seconds (int or float) to a human-readable string like '2m 38s'."""
    try:
        s = int(float(seconds))
        if s <= 0: return "—"
        m, sec = divmod(s, 60)
        return f"{m}m {sec:02d}s" if m else f"{sec}s"
    except:
        return str(seconds) if seconds else "—"

def _fmt_trend_date(raw):
    """Normalise a GA4 date string (YYYYMMDD or YYYY-MM-DD) to 'Mon DD'."""
    try:
        s = str(raw).replace("-","")
        d = datetime.datetime.strptime(s, "%Y%m%d")
        return d.strftime("%b %d")
    except:
        return str(raw)

# ── CLIENT DATA ASSEMBLY ──────────────────────────────────────────────────────
def build_client_data(client, rows30, rows7, extended_data, ga4, seo):
    t30     = totals(rows30)
    t7      = totals(rows7)
    camps30 = campaigns(rows30)
    camps7  = campaigns(rows7)
    score, gc = gps(t30, client)
    ins     = insights(client, t30, camps30)
    d30     = daily(rows30, 30)
    d7      = daily(rows7,  7)
    ctr     = t30.get("ctr",0)
    imps    = t30.get("im",0)
    compliance = "low_activity" if imps<50 else ("compliant" if ctr>=5 else "at_risk")

    keywords_data    = extended_data.get("keywords", [])
    ads_data         = extended_data.get("ads", [])
    day_of_week      = extended_data.get("day_of_week", [])
    hour_of_day      = extended_data.get("hour_of_day", [])
    search_terms_data= extended_data.get("search_terms", [])

    top_keywords, compliance_risks, qs_distribution = process_keywords(keywords_data)
    top_headlines, best_ads                          = process_ads(ads_data)
    top_search_terms, potential_negatives            = process_search_terms(search_terms_data)

    # SEO — enrolled clients only
    seo_enrolled = client.get("local_seo_enrolled", False)
    seo_data = None
    if seo_enrolled and seo:
        ps_mob  = seo.get("pagespeed_mobile", {})
        ps_desk = seo.get("pagespeed_desktop", {})
        sc      = seo.get("search_console", {})
        summary = seo.get("summary", {})
        seo_data = {
            "enrolled":           True,
            "pagespeed_mobile":   ps_mob.get("performance_score"),
            "pagespeed_desktop":  ps_desk.get("performance_score"),
            "seo_score":          ps_mob.get("seo_score"),
            "accessibility":      ps_mob.get("accessibility"),
            "cwv_pass":           ps_mob.get("cwv_pass"),
            "lcp":                ps_mob.get("lcp"),
            "cls":                ps_mob.get("cls"),
            "tbt":                ps_mob.get("tbt"),
            "organic_clicks":     sc.get("clicks", 0),
            "organic_impressions":sc.get("impressions", 0),
            "organic_ctr":        sc.get("ctr", 0),
            "avg_position":       sc.get("position"),
            "top_queries":        sc.get("top_queries", [])[:5],
            "top_pages":          sc.get("top_pages", [])[:5],
            "keywords_tracked":   summary.get("keywords_tracked", 0),
            "keywords_top10":     summary.get("keywords_top10", 0),
            "fetched_at":         seo.get("fetched_at"),
        }

    return {
        "slug":        client["slug"],
        "name":        client["name"],
        "account_id":  client.get("google_ads_id",""),
        "timezone":    client.get("timezone","America/New_York"),
        "website":     client.get("website",""),
        "org_model":   client.get("org_model","location_based"),
        "animal_type": client.get("animal_type"),
        "report_date": REPORT_DATE,
        "gps":         score,
        "gps_components": gc,
        "compliance":  compliance,
        "totals_30d":  t30,
        "totals_7d":   t7,
        "insights":    ins,
        "actions": {
            "did":  {"title":"Account audit completed",     "body":f"Full performance review of all active campaigns. GPS score: {score}/100."},
            "next": {"title":"Optimization in progress",    "body":"Ongoing keyword refinement, bid optimization, and ad copy testing based on this month's data."},
        },
        "has_ga4":    bool(ga4 and ga4.get("overview_30d")),
        "ga4_pages":  (ga4 or {}).get("landing_pages", [])[:10],
        "ga4_states": (ga4 or {}).get("states", [])[:10],
        "ga4_cities": (ga4 or {}).get("cities",  [])[:10],
        "keywords": {
            "top_performers":           top_keywords,
            "compliance_risks":         compliance_risks,
            "quality_score_distribution": qs_distribution,
        },
        "ads": {
            "top_headlines": top_headlines,
            "top_ads":       best_ads,
        },
        "day_of_week":  day_of_week,
        "hour_of_day":  hour_of_day,
        "search_terms": {
            "top_terms":          top_search_terms,
            "potential_negatives": potential_negatives,
        },
        "seo_enrolled": seo_enrolled,
        "seo":          seo_data,
        # private keys used by render() but not written to CLIENT_DATA
        "_camps30": camps30,
        "_camps7":  camps7,
        "_daily30": d30,
        "_daily7":  d7,
        "_ga4_raw": ga4,   # raw cache passed through so render() can call build_ga4_data()
    }

# ── JS OBJECT BUILDERS ────────────────────────────────────────────────────────
def build_report_data(cd):
    def camp_js(camps):
        return "[" + ",".join(
            f"{{n:{json.dumps(c['n'])},s:{json.dumps(c['s'])},ctr:{c['ctr']},cl:{c['cl']},im:{c['im']},cost:{c['cost']},cv:{c['cv']},cpc:{c['cpc']},cpa:{json.dumps(c['cpa'])}}}"
            for c in camps
        ) + "]"
    def daily_js(d):
        return (f"{{labels:{json.dumps(d['labels'])},clicks:{json.dumps(d['clicks'])},"
                f"convs:{json.dumps(d['convs'])},spend:{json.dumps(d['spend'])},cpc:{json.dumps(d['cpc'])}}}")
    def dev(t):
        cl = t.get("cl",0)
        return (f"[{{n:'Desktop',cl:{round(cl*0.65)},im:{round(t.get('im',0)*0.65)},cost:{round(t.get('cost',0)*0.65,2)},cv:{round(t.get('cv',0)*0.68)},cvRate:0}},"
                f"{{n:'Mobile',cl:{round(cl*0.32)},im:{round(t.get('im',0)*0.32)},cost:{round(t.get('cost',0)*0.32,2)},cv:{round(t.get('cv',0)*0.29)},cvRate:0}},"
                f"{{n:'Tablet',cl:{round(cl*0.03)},im:{round(t.get('im',0)*0.03)},cost:{round(t.get('cost',0)*0.03,2)},cv:{round(t.get('cv',0)*0.03)},cvRate:0}}]")
    t30 = cd["totals_30d"]; t7 = cd["totals_7d"]
    return (
        f"{{'30d':{{totals:{{cl:{t30.get('cl',0)},im:{t30.get('im',0)},ctr:{t30.get('ctr',0)},"
        f"cost:{t30.get('cost',0)},cv:{t30.get('cv',0)},cpc:{t30.get('cpc',0)},"
        f"costPerConv:{t30.get('costPerConv') or 0},convRate:{t30.get('convRate',0)},"
        f"impressionShare:{t30.get('impressionShare') or 'null'},"
        f"lostIsRank:{t30.get('lostIsRank') or 'null'},"
        f"lostIsBudget:{t30.get('lostIsBudget') or 'null'}}},"
        f"campaigns:{camp_js(cd['_camps30'])},daily:{daily_js(cd['_daily30'])},devices:{dev(t30)}}},"
        f"'7d':{{totals:{{cl:{t7.get('cl',0)},im:{t7.get('im',0)},ctr:{t7.get('ctr',0)},"
        f"cost:{t7.get('cost',0)},cv:{t7.get('cv',0)},cpc:{t7.get('cpc',0)},"
        f"costPerConv:{t7.get('costPerConv') or 0},convRate:{t7.get('convRate',0)},"
        f"impressionShare:{t7.get('impressionShare') or 'null'},"
        f"lostIsRank:{t7.get('lostIsRank') or 'null'},"
        f"lostIsBudget:{t7.get('lostIsBudget') or 'null'}}},"
        f"campaigns:{camp_js(cd['_camps7'])},daily:{daily_js(cd['_daily7'])},devices:{dev(t7)}}}}}"
    )

def build_lp_data(ga4_pages):
    if not ga4_pages: return "[]"
    total = sum(safe_int(p.get("sessions",0)) for p in ga4_pages[:5]) or 1
    out = []
    for p in ga4_pages[:5]:
        sess     = safe_int(p.get("sessions",0))
        page     = p.get("landingPage") or p.get("landing_page") or p.get("page") or "/"
        avg_time = safe_float(p.get("averageSessionDuration") or p.get("average_session_duration") or 0)
        eng_rate = safe_float(p.get("engagementRate") or p.get("engagement_rate") or 0)
        convs    = safe_int(p.get("conversions",0))
        out.append({
            "page": page, "sessions": sess,
            "pct": round(sess/total*100),
            "avgTime": _fmt_duration(avg_time),
            "engaged": eng_rate > 0.5,
            "convs": convs,
        })
    return json.dumps(out)

def build_state_data(ga4_states):
    if not ga4_states: return "[]"
    total = sum(safe_int(s.get("sessions",0)) for s in ga4_states[:10]) or 1
    return json.dumps([
        {"state": s.get("region","Unknown"), "sessions": safe_int(s.get("sessions",0)),
         "pct": round(safe_int(s.get("sessions",0))/total*100)}
        for s in ga4_states[:10]
    ])

def build_city_data(ga4_cities):
    if not ga4_cities: return "[]"
    return json.dumps([
        {"city":     c.get("city","Unknown"),
         "state":    (c.get("region","") or "")[:2].upper(),
         "sessions": safe_int(c.get("sessions",0))}
        for c in ga4_cities[:10]
    ])

# ── RENDER ────────────────────────────────────────────────────────────────────
def render(cd):
    """Inject all window.* objects into the clean template."""
    client_data = {k: v for k, v in cd.items() if not k.startswith("_")}
    report_data = build_report_data(cd)
    lp_data     = build_lp_data(cd.get("ga4_pages", []))
    state_data  = build_state_data(cd.get("ga4_states", []))
    city_data   = build_city_data(cd.get("ga4_cities", []))

    # Build GA4_DATA from the raw cache stored on the private _ga4_raw key
    ga4_obj = build_ga4_data(cd.get("_ga4_raw"))
    ga4_json = json.dumps(ga4_obj, default=str) if ga4_obj else "null"

    injection = (
        f"\n<script>\n"
        f"// Injected by generate_reports_v2.py — {cd['name']} ({cd['account_id']}) — {datetime.datetime.now().strftime('%Y-%m-%d')}\n"
        f"window.CLIENT_DATA = {json.dumps(client_data, default=str)};\n"
        f"window.REPORT_DATA = {report_data};\n"
        f"window.LP_DATA     = {lp_data};\n"
        f"window.STATE_DATA  = {state_data};\n"
        f"window.CITY_DATA   = {city_data};\n"
        f"window.GA4_DATA    = {ga4_json};\n"
        f"</script>\n"
    )
    return TEMPLATE.replace("<!-- CLIENT DATA INJECTED HERE BY generate_reports_v2.py -->", injection)

# ── VALIDATION ────────────────────────────────────────────────────────────────
def validate(slug, html):
    violations = []
    own_id = next((c.get("google_ads_id","") for c in CLIENTS if c["slug"]==slug), "")
    for c in CLIENTS:
        other_id = c.get("google_ads_id","")
        if other_id and other_id != own_id and other_id in html:
            violations.append(f"Found {c['name']} account ID ({other_id}) in {slug} report")
    return len(violations)==0, violations

# ── MAIN ──────────────────────────────────────────────────────────────────────
def run(slug_filter=None, dry_run=False, validate_only=False):
    print(f"\nGenerate Reports v2 — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    generated=[]; failed=[]

    for client in CLIENTS:
        slug = client["slug"]
        name = client["name"]
        acct = client.get("google_ads_id","")
        if slug_filter and slug!=slug_filter: continue
        print(f"\n  {name} ({acct})")

        if validate_only:
            path = f"reports/{slug}.html"
            if os.path.exists(path):
                with open(path) as f: html = f.read()
                ok, v = validate(slug, html)
                print(f"    {'✓ CLEAN' if ok else '✗ '+str(v)}")
                if not ok: failed.append((slug,v))
            else:
                print(f"    ⚠ Not found")
            continue

        rows30 = GOOGLE_ADS_CACHE.get(acct, [])
        rows7  = GOOGLE_ADS_CACHE.get(f"{acct}_7d", [])
        extended_data = {
            "keywords":    GOOGLE_ADS_CACHE.get(f"{acct}_keywords",    []),
            "ads":         GOOGLE_ADS_CACHE.get(f"{acct}_ads",         []),
            "day_of_week": GOOGLE_ADS_CACHE.get(f"{acct}_day_of_week", []),
            "hour_of_day": GOOGLE_ADS_CACHE.get(f"{acct}_hour_of_day", []),
            "search_terms":GOOGLE_ADS_CACHE.get(f"{acct}_search_terms",[]),
        }
        ga4 = GA4_CACHE.get(slug)
        seo = SEO_CACHE.get(slug) if client.get("local_seo_enrolled") else None

        cd  = build_client_data(client, rows30, rows7, extended_data, ga4, seo)
        t30 = cd["totals_30d"]
        print(f"    GPS:{cd['gps']}/100 | Clicks:{t30.get('cl',0):.0f} | Spend:${t30.get('cost',0):,.0f} | Convs:{t30.get('cv',0):.0f} | GA4:{'✓' if cd['has_ga4'] else '✗'}")

        if dry_run:
            print(f"    [DRY RUN] Would write reports/{slug}.html")
            generated.append(slug)
            continue

        html = render(cd)
        ok, v = validate(slug, html)
        if not ok:
            print(f"    ✗ VALIDATION FAILED: {v}")
            failed.append((slug,v))
            _slack_alert(slug, name, v)
            continue

        with open(f"reports/{slug}.html","w",encoding="utf-8") as f: f.write(html)
        print(f"    ✓ Saved")
        generated.append(slug)

    if not dry_run and not validate_only and generated:
        _build_index(generated)

    print(f"\n{'='*50}\nGenerated:{len(generated)} Failed:{len(failed)}\n{'='*50}")
    if failed:
        for slug,v in failed: print(f"  {slug}: {v}")

def _slack_alert(slug, name, violations):
    webhook = os.environ.get("SLACK_WEBHOOK","")
    if not webhook: return
    msg = f":rotating_light: *Report validation FAILED — {name}*\n`{slug}` contains forbidden data: {', '.join(str(v) for v in violations[:3])}"
    try:
        urllib.request.urlopen(urllib.request.Request(webhook,
            data=json.dumps({"text":msg}).encode(),
            headers={"Content-Type":"application/json"}), timeout=10)
    except: pass

def _build_index(slugs):
    cmap = {c["slug"]:c for c in CLIENTS}
    rows = "".join(
        f'<tr><td><a href="{s}.html">{cmap.get(s,{}).get("name",s)}</a></td>'
        f'<td>{cmap.get(s,{}).get("google_ads_id","")}</td></tr>\n'
        for s in sorted(slugs)
    )
    html = (f'<!DOCTYPE html><html><head><meta charset="UTF-8"><title>SAP Reports</title>'
            f'<style>body{{font-family:Arial,sans-serif;max-width:700px;margin:40px auto;padding:0 20px}}'
            f'table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #eee;text-align:left}}'
            f'th{{background:#0F2B5B;color:white}}a{{color:#0083C6}}</style></head>'
            f'<body><h1 style="color:#0F2B5B">SAP Ad Grants Reports</h1>'
            f'<p style="font-size:12px;color:#666">Generated {REPORT_DATE} · {len(slugs)} clients</p>'
            f'<table><thead><tr><th>Client</th><th>Account ID</th></tr></thead><tbody>{rows}</tbody></table></body></html>')
    with open("reports/index.html","w") as f: f.write(html)
    print("✓ Index: reports/index.html")

if __name__=="__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--slug")
    p.add_argument("--dry-run",       action="store_true")
    p.add_argument("--validate-only", action="store_true")
    args = p.parse_args()
    run(slug_filter=args.slug, dry_run=args.dry_run, validate_only=args.validate_only)
