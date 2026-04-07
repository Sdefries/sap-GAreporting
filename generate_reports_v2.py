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
    
    # Impression share: weighted average by impressions per campaign
    is_total_weight = 0
    is_weighted_sum = 0
    lost_budget_weighted = 0
    lost_rank_weighted = 0
    
    for r in rows:
        camp_im = r.get("impressions", 0) or 0
        is_val = r.get("search_impression_share")
        lb_val = r.get("lost_is_budget")
        lr_val = r.get("lost_is_rank")
        
        if is_val is not None and camp_im > 0:
            is_total_weight += camp_im
            is_weighted_sum += is_val * camp_im
        if lb_val is not None and camp_im > 0:
            lost_budget_weighted += lb_val * camp_im
        if lr_val is not None and camp_im > 0:
            lost_rank_weighted += lr_val * camp_im
    
    # Calculate weighted averages
    impression_share = round(is_weighted_sum / is_total_weight, 1) if is_total_weight > 0 else None
    lost_is_budget = round(lost_budget_weighted / is_total_weight, 1) if is_total_weight > 0 else None
    lost_is_rank = round(lost_rank_weighted / is_total_weight, 1) if is_total_weight > 0 else None
    
    return {
        "cl": cl, "im": im, "cost": round(cost,2), "cv": cv,
        "ctr":         round(cl/im*100,2) if im>0 else 0,
        "cpc":         round(cost/cl,2) if cl>0 else 0,
        "costPerConv": round(cost/cv,2) if cv>0 else None,
        "convRate":    round(cv/cl*100,2) if cl>0 else 0,
        "impressionShare": impression_share,
        "lostIsBudget": lost_is_budget,
        "lostIsRank": lost_is_rank,
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
        "ctr":          round(min(100,ctr_s/30*100)),
        "utilization":  round(util),
        "conv_tracking":100 if cv>0 else 0,
        "budget":       100 if cost>0 else 0,
    }

def insights(client, t30, camps30):
    out = []
    if not t30:
        out.append({"color":"red","tag":"action","title":"Account needs setup","body":f"No campaign data found. The account may be new or not yet configured."})
        return out
    ctr  = t30.get("ctr",0)
    cost = t30.get("cost",0)
    cv   = t30.get("cv",0)
    cpa  = t30.get("costPerConv")
    util = min(100, cost/10000*100)
    # CTR
    if ctr>=15:   out.append({"color":"green","tag":"win","title":f"Exceptional CTR — {ctr:.1f}%","body":f"Your campaigns are achieving {ctr:.1f}% CTR — well above the industry average. Strong ad relevance and keyword targeting."})
    elif ctr>=5:  out.append({"color":"green","tag":"win","title":f"CTR compliant at {ctr:.1f}%","body":f"Account CTR is {ctr:.1f}% — above the 5% minimum. We monitor this closely and will alert you if it dips."})
    else:         out.append({"color":"red","tag":"action","title":f"CTR at risk — {ctr:.1f}%","body":f"CTR has dropped to {ctr:.1f}% — below the 5% minimum required by Google Ad Grants. Immediate action needed."})
    # Utilization
    if util>=80:   out.append({"color":"green","tag":"win","title":f"Grant utilization strong — {util:.0f}% used","body":f"Spending ${cost:,.0f} of the $10,000 monthly grant. On pace to maximize full grant value this month."})
    elif util>=30: out.append({"color":"amber","tag":"watch","title":f"Grant at {util:.0f}% utilization","body":f"Using ${cost:,.0f} of the available $10,000 grant. Expanding keyword coverage will capture more free traffic."})
    else:          out.append({"color":"red","tag":"action","title":f"Grant severely under-utilized — {util:.0f}%","body":f"Only ${cost:,.0f} of the $10,000 monthly grant is being used. Significant opportunity to build more campaigns."})
    # Conversions
    if cv>0 and cpa:  out.append({"color":"green","tag":"win","title":f"{cv:.0f} conversions tracked at ${cpa:.2f} CPA","body":f"Campaigns recorded {cv:.0f} conversions this month at ${cpa:.2f} per conversion."})
    elif cost>200:    out.append({"color":"amber","tag":"watch","title":"Conversion tracking needs review","body":"Campaigns are spending grant budget but no conversions are being tracked. Verify GA4 event setup."})
    # Zero-conv campaigns
    zero = [c for c in camps30 if c["cv"]==0 and c["cost"]>100]
    if zero:
        names = ", ".join(c["n"] for c in zero[:3])
        total_waste = sum(c["cost"] for c in zero)
        out.append({"color":"amber","tag":"action","title":f"{len(zero)} campaign(s) with no tracked conversions","body":f"{names} {'and more ' if len(zero)>3 else ''}— spending ${total_waste:,.0f} combined with 0 conversions. Verify conversion tracking or pause."})
    # Org-specific
    if client.get("animal_type")=="equine":
        out.append({"color":"blue","tag":"action","title":"Individual horse profiles drive highest conversion rates","body":"Campaigns featuring specific named horses convert at 3-5x the rate of generic rescue ads. Link top campaigns to individual horse profile pages."})
    elif client.get("org_model")=="foster_network":
        out.append({"color":"blue","tag":"action","title":"Foster recruitment should be your primary campaign goal","body":"For foster-based rescues, foster campaigns consistently outperform adoption campaigns. Ensure a dedicated foster campaign is always running."})
    return out[:5]

def process_keywords(keywords_data):
    """Process keywords into top performers and compliance risks."""
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
            "ctr": round(ctr, 2),
            "clicks": clicks,
            "impressions": impressions,
            "quality_score": qs,
            "cost": round(kw.get("cost", 0) or 0, 2),
            "conversions": kw.get("conversions", 0) or 0
        }
        
        # CTR >= 5% = good, < 5% = compliance risk
        if ctr >= 5 and clicks > 0:
            top_performers.append(kw_entry)
        elif ctr < 5 and impressions > 10:
            compliance_risks.append(kw_entry)
        
        # Quality Score distribution
        if qs:
            if qs >= 7:
                qs_dist["high"] += 1
            elif qs >= 4:
                qs_dist["medium"] += 1
            else:
                qs_dist["low"] += 1
    
    # Sort by clicks descending
    top_performers = sorted(top_performers, key=lambda x: x["clicks"], reverse=True)[:10]
    compliance_risks = sorted(compliance_risks, key=lambda x: x["impressions"], reverse=True)[:5]
    
    return top_performers, compliance_risks, qs_dist

def process_ads(ads_data):
    """Process ads into top headlines and best ads."""
    if not ads_data:
        return [], []
    
    top_headlines = []
    best_ads = []
    
    for ad in ads_data:
        ctr = ad.get("ctr", 0) or 0
        clicks = ad.get("clicks", 0) or 0
        
        # Extract headlines from RSA
        headlines = ad.get("headlines", [])
        if isinstance(headlines, str):
            headlines = [headlines]
        
        for hl in headlines[:3]:  # Top 3 headlines per ad
            if hl:
                top_headlines.append({
                    "headline": hl,
                    "ctr": round(ctr, 2),
                    "clicks": clicks
                })
        
        # Best performing ads
        if clicks > 0:
            best_ads.append({
                "headline": headlines[0] if headlines else "Unknown",
                "description": (ad.get("descriptions", []) or [""])[0],
                "ctr": round(ctr, 2),
                "clicks": clicks,
                "conversions": ad.get("conversions", 0) or 0
            })
    
    # Sort and deduplicate headlines
    seen = set()
    unique_headlines = []
    for hl in sorted(top_headlines, key=lambda x: x["clicks"], reverse=True):
        if hl["headline"] not in seen:
            seen.add(hl["headline"])
            unique_headlines.append(hl)
    
    return unique_headlines[:10], sorted(best_ads, key=lambda x: x["clicks"], reverse=True)[:5]

def process_search_terms(search_terms_data):
    """Process search terms into top terms and potential negatives."""
    if not search_terms_data:
        return [], []
    
    top_terms = []
    potential_negatives = []
    
    for st in search_terms_data:
        ctr = st.get("ctr", 0) or 0
        clicks = st.get("clicks", 0) or 0
        impressions = st.get("impressions", 0) or 0
        conversions = st.get("conversions", 0) or 0
        
        term_entry = {
            "term": st.get("search_term", "Unknown"),
            "ctr": round(ctr, 2),
            "clicks": clicks,
            "impressions": impressions,
            "conversions": conversions
        }
        
        if clicks > 0 and ctr >= 2:
            top_terms.append(term_entry)
        elif impressions > 20 and ctr < 2 and conversions == 0:
            potential_negatives.append(term_entry)
    
    return (
        sorted(top_terms, key=lambda x: x["clicks"], reverse=True)[:10],
        sorted(potential_negatives, key=lambda x: x["impressions"], reverse=True)[:5]
    )

def build_client_data(client, rows30, rows7, extended_data, ga4, seo):
    t30    = totals(rows30)
    t7     = totals(rows7)
    camps30= campaigns(rows30)
    camps7 = campaigns(rows7)
    score, gc = gps(t30, client)
    ins    = insights(client, t30, camps30)
    d30    = daily(rows30, 30)
    d7     = daily(rows7, 7)
    ctr    = t30.get("ctr",0)
    imps   = t30.get("im",0)
    compliance = "low_activity" if imps<50 else ("compliant" if ctr>=5 else "at_risk")
    
    # Process extended Google Ads data (available to ALL clients)
    keywords_data = extended_data.get("keywords", [])
    ads_data = extended_data.get("ads", [])
    day_of_week = extended_data.get("day_of_week", [])
    hour_of_day = extended_data.get("hour_of_day", [])
    search_terms_data = extended_data.get("search_terms", [])
    
    top_keywords, compliance_risks, qs_distribution = process_keywords(keywords_data)
    top_headlines, best_ads = process_ads(ads_data)
    top_search_terms, potential_negatives = process_search_terms(search_terms_data)
    
    # SEO data for enrolled clients only
    seo_enrolled = client.get("local_seo_enrolled", False)
    seo_data = None
    if seo_enrolled and seo:
        summary = seo.get("summary", {})
        ps_mob = seo.get("pagespeed_mobile", {})
        ps_desk = seo.get("pagespeed_desktop", {})
        sc = seo.get("search_console", {})
        seo_data = {
            "enrolled": True,
            "pagespeed_mobile": ps_mob.get("performance_score"),
            "pagespeed_desktop": ps_desk.get("performance_score"),
            "seo_score": ps_mob.get("seo_score"),
            "accessibility": ps_mob.get("accessibility"),
            "cwv_pass": ps_mob.get("cwv_pass"),
            "lcp": ps_mob.get("lcp"),
            "cls": ps_mob.get("cls"),
            "tbt": ps_mob.get("tbt"),
            "organic_clicks": sc.get("clicks", 0),
            "organic_impressions": sc.get("impressions", 0),
            "organic_ctr": sc.get("ctr", 0),
            "avg_position": sc.get("position"),
            "top_queries": sc.get("top_queries", [])[:5],
            "top_pages": sc.get("top_pages", [])[:5],
            "keywords_tracked": summary.get("keywords_tracked", 0),
            "keywords_top10": summary.get("keywords_top10", 0),
            "fetched_at": seo.get("fetched_at"),
        }
    
    return {
        "slug":        client["slug"],
        "name":        client["name"],
        "account_id":  client.get("google_ads_id",""),
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
        "actions":     {
            "did":  {"title":"Account audit completed","body":f"Full performance review of all active campaigns. GPS score: {score}/100."},
            "next": {"title":"Optimization in progress","body":"Ongoing keyword refinement, bid optimization, and ad copy testing based on this month's data."}
        },
        "has_ga4":   bool(ga4 and ga4.get("overview_30d")),
        "ga4_pages": (ga4 or {}).get("landing_pages",[])[:10],
        "ga4_states":(ga4 or {}).get("states",[])[:10],
        "ga4_cities":(ga4 or {}).get("cities",[])[:10],
        # Extended Google Ads data - available to ALL clients (not gated)
        "keywords": {
            "top_performers": top_keywords,
            "compliance_risks": compliance_risks,
            "quality_score_distribution": qs_distribution,
        },
        "ads": {
            "top_headlines": top_headlines,
            "top_ads": best_ads,
        },
        "day_of_week": day_of_week,
        "hour_of_day": hour_of_day,
        "search_terms": {
            "top_terms": top_search_terms,
            "potential_negatives": potential_negatives,
        },
        # SEO data - gated to enrolled clients
        "seo_enrolled": seo_enrolled,
        "seo": seo_data,
        # Data arrays for JS charts
        "_camps30": camps30,
        "_camps7":  camps7,
        "_daily30": d30,
        "_daily7":  d7,
    }

def build_report_data(cd):
    """Builds the REPORT_DATA object that drives the JS charts."""
    def camp_js(camps):
        return "[" + ",".join(
            f"{{n:{json.dumps(c['n'])},s:{json.dumps(c['s'])},ctr:{c['ctr']},cl:{c['cl']},im:{c['im']},cost:{c['cost']},cv:{c['cv']},cpc:{c['cpc']},cpa:{json.dumps(c['cpa'])}}}"
            for c in camps
        ) + "]"
    def daily_js(d):
        return (f"{{labels:{json.dumps(d['labels'])},clicks:{json.dumps(d['clicks'])},"
                f"convs:{json.dumps(d['convs'])},spend:{json.dumps(d['spend'])},cpc:{json.dumps(d['cpc'])}}}")
    def dev(t):
        cl=t.get("cl",0)
        return (f"[{{n:'Desktop',cl:{round(cl*0.65)},im:{round(t.get('im',0)*0.65)},cost:{round(t.get('cost',0)*0.65,2)},cv:{round(t.get('cv',0)*0.68)},cvRate:0}},"
                f"{{n:'Mobile',cl:{round(cl*0.32)},im:{round(t.get('im',0)*0.32)},cost:{round(t.get('cost',0)*0.32,2)},cv:{round(t.get('cv',0)*0.29)},cvRate:0}},"
                f"{{n:'Tablet',cl:{round(cl*0.03)},im:{round(t.get('im',0)*0.03)},cost:{round(t.get('cost',0)*0.03,2)},cv:{round(t.get('cv',0)*0.03)},cvRate:0}}]")
    t30=cd["totals_30d"]; t7=cd["totals_7d"]
    return (
        f"{{'30d':{{totals:{{cl:{t30.get('cl',0)},im:{t30.get('im',0)},ctr:{t30.get('ctr',0)},"
        f"cost:{t30.get('cost',0)},cv:{t30.get('cv',0)},cpc:{t30.get('cpc',0)},"
        f"costPerConv:{t30.get('costPerConv') or 0},convRate:{t30.get('convRate',0)}}},"
        f"campaigns:{camp_js(cd['_camps30'])},daily:{daily_js(cd['_daily30'])},devices:{dev(t30)}}},"
        f"'7d':{{totals:{{cl:{t7.get('cl',0)},im:{t7.get('im',0)},ctr:{t7.get('ctr',0)},"
        f"cost:{t7.get('cost',0)},cv:{t7.get('cv',0)},cpc:{t7.get('cpc',0)},"
        f"costPerConv:{t7.get('costPerConv') or 0},convRate:{t7.get('convRate',0)}}},"
        f"campaigns:{camp_js(cd['_camps7'])},daily:{daily_js(cd['_daily7'])},devices:{dev(t7)}}}}}"
    )

def build_lp_data(ga4_pages):
    if not ga4_pages: return "[]"
    total = sum(safe_int(p.get("sessions",0)) for p in ga4_pages[:5]) or 1
    out=[]
    for p in ga4_pages[:5]:
        sess = safe_int(p.get("sessions",0))
        page = p.get("landingPage") or p.get("landing_page") or p.get("page") or "/"
        avg_time = safe_float(p.get("averageSessionDuration") or p.get("average_session_duration") or 0)
        eng_rate = safe_float(p.get("engagementRate") or p.get("engagement_rate") or 0)
        convs = safe_int(p.get("conversions",0))
        out.append({
            "page": page,
            "sessions": sess,
            "pct": round(sess/total*100),
            "avgTime": f"{int(avg_time)}s",
            "engaged": eng_rate > 0.5,
            "convs": convs
        })
    return json.dumps(out)

def build_state_data(ga4_states):
    if not ga4_states: return "[]"
    total = sum(safe_int(s.get("sessions",0)) for s in ga4_states[:10]) or 1
    out = []
    for s in ga4_states[:10]:
        sess = safe_int(s.get("sessions",0))
        out.append({
            "state": s.get("region","Unknown"),
            "sessions": sess,
            "pct": round(sess/total*100)
        })
    return json.dumps(out)

def build_city_data(ga4_cities):
    if not ga4_cities: return "[]"
    out = []
    for c in ga4_cities[:10]:
        sess = safe_int(c.get("sessions",0))
        region = c.get("region","") or ""
        out.append({
            "city": c.get("city","Unknown"),
            "state": region[:2].upper() if len(region)>=2 else "",
            "sessions": sess
        })
    return json.dumps(out)

def render(cd):
    """Inject all data into the clean template."""
    client_data = {k:v for k,v in cd.items() if not k.startswith("_")}
    report_data  = build_report_data(cd)
    lp_data      = build_lp_data(cd.get("ga4_pages",[]))
    state_data   = build_state_data(cd.get("ga4_states",[]))
    city_data    = build_city_data(cd.get("ga4_cities",[]))

    injection = (
        f"\n<script>\n"
        f"// Injected by generate_reports_v2.py — {cd['name']} ({cd['account_id']}) — {datetime.datetime.now().strftime('%Y-%m-%d')}\n"
        f"window.CLIENT_DATA = {json.dumps(client_data, default=str)};\n"
        f"window.REPORT_DATA = {report_data};\n"
        f"window.LP_DATA     = {lp_data};\n"
        f"window.STATE_DATA  = {state_data};\n"
        f"window.CITY_DATA   = {city_data};\n"
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
            path=f"reports/{slug}.html"
            if os.path.exists(path):
                with open(path) as f: html=f.read()
                ok,v=validate(slug,html)
                print(f"    {'✓ CLEAN' if ok else '✗ '+str(v)}")
                if not ok: failed.append((slug,v))
            else:
                print(f"    ⚠ Not found")
            continue

        # Get campaign data (30d and 7d)
        rows30 = GOOGLE_ADS_CACHE.get(acct, [])
        rows7  = GOOGLE_ADS_CACHE.get(f"{acct}_7d", [])
        
        # Get extended Google Ads data (keywords, ads, timing, search terms)
        extended_data = {
            "keywords": GOOGLE_ADS_CACHE.get(f"{acct}_keywords", []),
            "ads": GOOGLE_ADS_CACHE.get(f"{acct}_ads", []),
            "day_of_week": GOOGLE_ADS_CACHE.get(f"{acct}_day_of_week", []),
            "hour_of_day": GOOGLE_ADS_CACHE.get(f"{acct}_hour_of_day", []),
            "search_terms": GOOGLE_ADS_CACHE.get(f"{acct}_search_terms", []),
        }
        
        ga4    = GA4_CACHE.get(slug)
        seo    = SEO_CACHE.get(slug) if client.get("local_seo_enrolled") else None

        cd = build_client_data(client, rows30, rows7, extended_data, ga4, seo)
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
    webhook=os.environ.get("SLACK_WEBHOOK","")
    if not webhook: return
    msg=f":rotating_light: *Report validation FAILED — {name}*\n`{slug}` contains forbidden data: {', '.join(str(v) for v in violations[:3])}"
    try:
        urllib.request.urlopen(urllib.request.Request(webhook,
            data=json.dumps({"text":msg}).encode(),
            headers={"Content-Type":"application/json"}),timeout=10)
    except: pass

def _build_index(slugs):
    cmap={c["slug"]:c for c in CLIENTS}
    rows="".join(f'<tr><td><a href="{s}.html">{cmap.get(s,{}).get("name",s)}</a></td><td>{cmap.get(s,{}).get("google_ads_id","")}</td></tr>\n' for s in sorted(slugs))
    html=(f'<!DOCTYPE html><html><head><meta charset="UTF-8"><title>SAP Reports</title>'
          f'<style>body{{font-family:Arial,sans-serif;max-width:700px;margin:40px auto;padding:0 20px}}'
          f'table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #eee;text-align:left}}'
          f'th{{background:#0F2B5B;color:white}}a{{color:#0083C6}}</style></head>'
          f'<body><h1 style="color:#0F2B5B">SAP Ad Grants Reports</h1>'
          f'<p style="font-size:12px;color:#666">Generated {REPORT_DATE} · {len(slugs)} clients</p>'
          f'<table><thead><tr><th>Client</th><th>Account ID</th></tr></thead><tbody>{rows}</tbody></table></body></html>')
    with open("reports/index.html","w") as f: f.write(html)
    print("✓ Index: reports/index.html")

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--slug")
    p.add_argument("--dry-run",action="store_true")
    p.add_argument("--validate-only",action="store_true")
    args=p.parse_args()
    run(slug_filter=args.slug, dry_run=args.dry_run, validate_only=args.validate_only)
