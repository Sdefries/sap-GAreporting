"""
demo_data.py
─────────────────────────────────────────────────────────────────────────────
Sample data for the demo client (a client with "demo": true in clients.json),
so every section of a report can be shown to prospects without using a real
client's numbers. Nothing here is written to the data caches: the report
generator calls build() for demo clients only, and every fetcher skips them.

The organization, its website and its competitors are fictional (.example
domains). AI answers and keyword rankings go through the real scoring code,
so the demo behaves exactly like a live report. Same seed every run, with
dates relative to today, so the demo always looks current.
"""

import datetime
import math
import random

import aeo_audit
import fetch_ai_visibility as F
import fetch_local as L
import fetch_seo as S

TODAY = datetime.date.today()
D = lambda days: (TODAY - datetime.timedelta(days=days)).isoformat()


def build(client):
    rnd = random.Random(7)
    site = F.domain_of(client.get("website", "")) or "maplewoodpaws.example"
    brand = (client.get("ai_tracking") or {}).get("brand_names", [client["name"]])[0]
    comps = client.get("competitors") or []
    out = {}

    # ── Google Ads ────────────────────────────────────────────────────────
    camps = [("Adopt a Dog — Search", 0.30, 0.14, 2.10, 0.22), ("Monthly Giving — PMax", 0.24, 0.11, 2.60, 0.18),
             ("Foster Program", 0.16, 0.16, 1.80, 0.20), ("Volunteer — Search", 0.12, 0.12, 1.60, 0.12),
             ("Spring Adoption Event", 0.10, 0.18, 1.40, 0.15), ("Brand", 0.08, 0.31, 0.90, 0.30)]
    def rows(days, clicks_total):
        out_rows = []
        for name, share, ctr, cpc, cvr in camps:
            cl = round(clicks_total * share * rnd.uniform(.9, 1.1))
            out_rows.append({"account_id": "demo", "campaign": name, "campaign_status": "ENABLED", "clicks": float(cl),
                             "impressions": float(round(cl / ctr)), "ctr": ctr, "cost": round(cl * cpc, 2),
                             "conversions": float(round(cl * cvr)), "search_impression_share": 18.0,
                             "lost_is_budget": 22.5, "lost_is_rank": 59.5})
        return out_rows
    out["rows30"], out["rows7"] = rows(30, 4200), rows(7, 1000)

    daily = []
    for i in range(30, 0, -1):
        day = TODAY - datetime.timedelta(days=i)
        wk = 1.2 if day.weekday() in (1, 2) else 0.8 if day.weekday() >= 5 else 1.0
        cl = round(140 * wk * rnd.uniform(.82, 1.18) * (1 + (30 - i) * 0.006))
        daily.append({"date": day.isoformat(), "cl": cl, "im": cl * 8, "cost": round(cl * rnd.uniform(1.8, 2.3), 2),
                      "cv": round(cl * rnd.uniform(.15, .22), 1)})
    kws = ["adopt a dog denver", "dog rescue near me", "foster a dog", "puppies for adoption denver", "donate to animal rescue",
           "volunteer animal shelter", "adopt a senior dog", "rescue dogs colorado", "dog adoption events", "monthly donation dog rescue",
           "animal rescue near me", "adopt a cat denver", "dog foster program", "sponsor a shelter dog", "maplewood paws"]
    keywords = []
    for i, k in enumerate(kws):
        im = rnd.randint(300, 2600); ctr = rnd.uniform(4.2, 22) if i != 9 else 3.1
        cl = round(im * ctr / 100)
        keywords.append({"ad_group": "Core", "keyword": k, "match_type": rnd.choice(["PHRASE", "EXACT", "BROAD"]), "status": "ENABLED",
                         "quality_score": [8, 7, 6, 9, 5, 7, 8, 6, 7, 2, 6, 7, 8, 7, 10][i], "clicks": cl, "impressions": im,
                         "ctr": round(ctr, 2), "cost": round(cl * 1.9, 2), "conversions": round(cl * .18, 1), "avg_cpc": 1.9})
    heads = ["Adopt a Rescue Dog Today", "Foster a Dog in Denver", "Give Monthly, Save Lives", "Meet Our Adoptable Dogs",
             "Volunteer With Rescue Pups", "Sponsor a Shelter Dog"]
    ads = [{"ad_group": "Core", "headlines": heads[i:i + 3] + heads[:i], "descriptions": [
        "Every adoption opens a kennel for the next dog in need. Meet them online today.",
        "Foster for two weeks or two months. We cover food, vet care and supplies."][i % 2:i % 2 + 1],
        "clicks": 400 - i * 60, "impressions": 3000 - i * 300, "ctr": round((400 - i * 60) / (3000 - i * 300) * 100, 2),
        "conversions": 70 - i * 9} for i in range(4)]
    dows = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow = [{"day": d, "clicks": c, "impressions": c * 8, "cost": round(c * 2, 2), "conversions": round(c * .18), "ctr": round(100 / 8, 2)}
           for d, c in zip(dows, [610, 702, 688, 640, 560, 470, 498])]
    hour = [{"hour": h, "clicks": max(4, round(220 * math.exp(-((h - 13) ** 2) / 28) + rnd.randint(0, 12))), "impressions": 0,
             "cost": 0, "conversions": 0, "ctr": 12.0} for h in range(24)]
    terms = [("adopt a dog denver", 96, 14.8, 21), ("dog rescue near me", 74, 11.2, 12), ("foster dogs denver", 51, 17.5, 11),
             ("puppies for adoption", 44, 9.8, 6), ("senior dogs for adoption", 31, 19.4, 7), ("donate to dog rescue", 28, 12.1, 9),
             ("animal shelter volunteer", 22, 10.5, 4), ("free puppies", 6, 1.9, 0), ("dog grooming denver", 3, 0.8, 0)]
    search_terms = [{"search_term": t, "matched_keyword": "adopt a dog denver", "clicks": c, "impressions": round(c / ctr * 100),
                     "ctr": ctr, "conversions": float(cv)} for t, c, ctr, cv in terms]
    out["extended"] = {"keywords": keywords, "ads": ads, "day_of_week": dow, "hour_of_day": hour,
                       "search_terms": search_terms, "daily": daily}
    dev = lambda f: [{"n": "Mobile", "cl": round(2730 * f), "im": round(21000 * f), "cost": round(5600 * f, 2), "cv": round(480 * f)},
                     {"n": "Desktop", "cl": round(1260 * f), "im": round(10500 * f), "cost": round(2800 * f, 2), "cv": round(290 * f)},
                     {"n": "Tablet", "cl": round(210 * f), "im": round(1900 * f), "cost": round(430 * f, 2), "cv": round(30 * f)}]
    out["devices"] = {"30d": dev(1), "7d": dev(7 / 30)}

    # ── GA4 ───────────────────────────────────────────────────────────────
    pages = [("/adopt", 2410, 0.071), ("/", 1980, 0.024), ("/foster", 990, 0.094), ("/donate", 760, 0.21),
             ("/dogs/biscuit", 520, 0.048), ("/volunteer", 410, 0.083), ("/events/spring-adoption-day", 380, 0.11),
             ("/blog/first-week-with-a-rescue-dog", 340, 0.006), ("/about", 220, 0.014), ("/dogs/juniper", 190, 0.052)]
    channels = ["Paid Search", "Organic Search", "Direct", "Email", "Organic Social", "Referral"]
    lp = []
    for i, (p, s, cr) in enumerate(pages):
        lp.append({"landingPage": p, "sessions": str(s), "averageSessionDuration": str(rnd.randint(45, 210)),
                   "bounceRate": str(rnd.uniform(.28, .55)), "engagementRate": str(rnd.uniform(.48, .74)),
                   "conversions": str(round(s * cr)), "conv_rate": cr, "prev_sessions": 0 if i == 6 else round(s * rnd.uniform(.75, 1.15)),
                   "top_channel": channels[0] if i in (0, 2, 3) else rnd.choice(channels)})
    sessions = sum(int(x["sessions"]) for x in lp) + 1100
    conv = sum(int(x["conversions"]) for x in lp)
    overview = {"sessions": str(sessions), "totalUsers": str(round(sessions * .82)), "newUsers": str(round(sessions * .64)),
                "screenPageViews": str(round(sessions * 2.4)), "averageSessionDuration": "118.4", "bounceRate": "0.392",
                "engagementRate": "0.608", "conversions": str(conv), "sessions_delta": 8.4, "users_delta": 6.9,
                "new_users_delta": 11.2, "pageviews_delta": 5.1, "engagement_delta": 2.4, "bounce_delta": -3.6,
                "duration_delta": 4.0, "conv_delta": 12.7}
    trend = []
    for i in range(30, 0, -1):
        day = TODAY - datetime.timedelta(days=i)
        trend.append({"date": day.strftime("%b %d"), "sessions": round(sessions / 30 * (1.15 if day.weekday() < 5 else .78) * rnd.uniform(.85, 1.15))})
    utm = [("google / cpc", 4120), ("google / organic", 2210), ("(direct) / (none)", 1180), ("mailchimp / email", 640),
           ("instagram / social", 410), ("facebook / social", 260), ("chatgpt.com / referral", 96), ("petfinder.com / referral", 88)]
    tot = sum(n for _, n in utm)
    out["ga4"] = {
        "ga4_id": "demo", "client": client["slug"], "fetched_at": datetime.datetime.now().isoformat(),
        "overview_30d": overview, "overview_7d": {k: v for k, v in overview.items() if not k.endswith("_delta")},
        "sessions_trend": trend,
        "utm_sources": [{"name": n, "sessions": s, "pct": round(s / tot * 100, 1), "utm": "utm_source={}&utm_medium={}".format(*n.split(" / "))} for n, s in utm],
        "devices": {"mobile": {"sessions": round(sessions * .63), "users": round(sessions * .52), "conversions": round(conv * .55), "engagement_rate": 57.2, "avg_time": "1m 41s", "bounce_rate": 42.8, "share": 63.0},
                    "desktop": {"sessions": round(sessions * .32), "users": round(sessions * .26), "conversions": round(conv * .4), "engagement_rate": 68.9, "avg_time": "2m 37s", "bounce_rate": 31.1, "share": 32.0},
                    "tablet": {"sessions": round(sessions * .05), "users": round(sessions * .04), "conversions": round(conv * .05), "engagement_rate": 61.0, "avg_time": "2m 02s", "bounce_rate": 39.0, "share": 5.0}},
        "browsers": [{"name": "Chrome", "pct": 48.2}, {"name": "Safari", "pct": 39.6}, {"name": "Edge", "pct": 6.1},
                     {"name": "Samsung Internet", "pct": 3.4}, {"name": "Firefox", "pct": 2.7}],
        "demographics": {
            "gender": [{"name": "Female", "value": 68.4}, {"name": "Male", "value": 31.6}],
            "gender_engagement": [{"metric": "Eng. Rate", "female": 63.1, "male": 56.2}, {"metric": "Conv. Rate", "female": 5.8, "male": 4.4}],
            "age_groups": [{"age": a, "sessions": s, "share": sh, "conv_rate": cr} for a, s, sh, cr in
                           [("18-24", 210, 9.1, 2.9), ("25-34", 520, 22.4, 5.1), ("35-44", 480, 20.7, 6.2),
                            ("45-54", 400, 17.2, 6.8), ("55-64", 380, 16.4, 7.4), ("65+", 330, 14.2, 5.6),
                            ("unknown", 9100, 0, None)]]},
        "landing_pages": lp,
        "states": [{"region": r, "sessions": str(s), "totalUsers": str(round(s * .85))} for r, s in
                   [("Colorado", 6900), ("Wyoming", 610), ("California", 420), ("Texas", 300), ("Utah", 260), ("Arizona", 190), ("New Mexico", 150)]],
        "cities": [{"city": c, "region": "Colorado", "sessions": str(s), "totalUsers": str(round(s * .85))} for c, s in
                   [("Denver", 3100), ("Aurora", 980), ("Lakewood", 720), ("Boulder", 640), ("Arvada", 510), ("Littleton", 470), ("Fort Collins", 330)]],
        "channels": [{"sessionDefaultChannelGroup": c, "sessions": str(s), "totalUsers": str(round(s * .85)), "conversions": str(round(s * .05))}
                     for c, s in [("Paid Search", 4120), ("Organic Search", 2300), ("Direct", 1180), ("Email", 640), ("Organic Social", 670), ("Referral", 330)]],
        "ai_referrals": {
            "by_source_30d": [{"source": "ChatGPT", "sessions": 96, "engaged": 71, "conversions": 7},
                              {"source": "Perplexity", "sessions": 22, "engaged": 17, "conversions": 2},
                              {"source": "Gemini", "sessions": 14, "engaged": 9, "conversions": 1},
                              {"source": "Copilot", "sessions": 6, "engaged": 4, "conversions": 0}],
            "total_30d": 138, "prior_total_30d": 97,
            "prior_by_source_30d": {"ChatGPT": 70, "Perplexity": 15, "Gemini": 9, "Copilot": 3},
            "weekly": [{"week": (TODAY - datetime.timedelta(weeks=12 - w)).strftime("%G%V"), "ChatGPT": 10 + w * 1, "Perplexity": 2 + w // 3, "Gemini": 1 + w // 4}
                       for w in range(12)],
            "top_pages": [{"page": "/adopt", "sessions": 52}, {"page": "/foster", "sessions": 31}, {"page": "/donate", "sessions": 19}],
        },
    }

    # ── AI visibility (synthetic answers through the real scoring code) ───
    cfg = F.client_config(client)
    peers = [F.domain_of(c["domain"]) for c in comps] + ["petfinder.com", "yelp.com", "reddit.com", "adoptapet.com"]
    def answer(eng, prompt, run):
        if eng in ("ai_overviews", "ai_mode") and rnd.random() < .18:
            return F.answer(status="no_answer")
        vis = rnd.random() < (.3 + .1 * run + (.25 if eng in ("gemini", "perplexity") else 0))
        sites = rnd.sample(peers, 3) + ([site] if vis and rnd.random() < .6 else [])
        names = [c["name"] for c in comps[:2]] + ([brand] if vis else [])
        text = (f"Good options for \u201c{prompt}\u201d:\n\n" + "\n".join(f"- **{n}**: a well-reviewed local rescue with dogs listed online." for n in names)
                + "\n\nPetfinder also lists adoptable dogs across the Denver area.")
        return F.answer(text, [{"url": f"https://www.{s}/page"} for s in sites])
    runs = []
    for run, days in ((0, 21), (1, 14), (2, 7), (3, 0)):
        res = {p: {e: F.score_answer(answer(e, p, run), cfg) for e in F.ENGINES} for p in cfg["prompts"]}
        if run < 3:
            for per in res.values():
                for v in per.values():
                    v.pop("excerpt", None)
        runs.append({"date": D(days), "checked_at": D(days) + "T09:00:00", "engines": F.ENGINES, "live_engines": F.ENGINES,
                     "prompts": cfg["prompts"], "results": res})
    home = (f"<html><head><title>{brand} | Dog Rescue in Denver</title>"
            f"<meta name='description' content='{brand} rescues, fosters and rehomes dogs across Denver.'>"
            '<script type="application/ld+json">{"@type":"NGO","name":"' + brand + '"}</script></head>'
            f"<body><h1>{brand}</h1><p>We rescue dogs from overcrowded shelters across Colorado and place them in loving homes. "
            "Since 2014 our volunteers have fostered and rehomed more than 3,000 dogs.</p>"
            "<a href='/adopt'>Adopt</a><a href='/donate'>Donate</a><p>Call 303-555-0142</p></body></html>")
    pages_ok = {"/": home, "/robots.txt": "User-agent: *\nAllow: /\nUser-agent: GPTBot\nDisallow: /\nSitemap: https://" + site + "/sitemap.xml",
                "/sitemap.xml": "<urlset><url><loc>https://" + site + "/</loc></url></urlset>"}
    def fake_fetch(url):  # the demo site, served from the strings above
        parts = url.split("/", 3)
        path = "/" + (parts[3] if len(parts) > 3 else "")
        return (200, pages_ok[path], url) if path in pages_ok else (404, "", url)
    audit = aeo_audit.audit_site("https://" + site, fake_fetch)
    out["ai"] = {
        "brand_names": cfg["brand_names"], "domain": site, "competitors": cfg["competitors"], "runs": runs, "audit": audit,
        "prompt_ideas": {"paa": [{"q": q, "from": "Google · People also ask"} for q in
                                 ["How much does it cost to adopt a dog in Denver?", "Can I foster a dog if I rent?", "What should I bring when I adopt a dog?"]],
                         "ai": ["Which Denver rescues take monthly donations?", "Where can I foster a senior dog in Colorado?",
                                "Are there dog adoption events in Denver this weekend?"], "ai_generated_at": D(0)},
        "action_plan": {"overall": "AI answers lean on Petfinder and two larger rescues. Getting listed there and publishing clear foster and senior-dog pages closes most gaps.",
                        "generated_at": D(0), "actions": [
            {"prompt": cfg["prompts"][1], "priority": "high", "why": "ChatGPT and Gemini name two other rescues and cite their foster pages; we have no page that answers this directly.",
             "sources_to_target": ["petfinder.com", "reddit.com"], "page_title": "Foster a Dog in Denver: How It Works",
             "url_slug": "/foster-a-dog-denver", "outline": ["Who can foster", "What we cover", "How long it lasts", "Apply in 5 minutes"],
             "faq": ["Can I foster if I rent?", "Do I pay for vet care?"], "quick_wins": ["Add the foster page to Petfinder's organization profile"]},
            {"prompt": cfg["prompts"][2], "priority": "medium", "why": "Google's AI Overview cites Charity Navigator and a national charity, not local rescues.",
             "sources_to_target": ["charitynavigator.org"], "page_title": "Where Your Donation Goes",
             "url_slug": "/donate/impact", "outline": ["Cost to rescue one dog", "Monthly giving", "Annual report"],
             "faq": ["Is my donation tax-deductible?"], "quick_wins": ["Complete the Charity Navigator profile"]}]},
        "accuracy": {"summary": "Mostly accurate. One answer lists an old address.", "checked": 9, "generated_at": D(0), "issues": [
            {"prompt": cfg["prompts"][0], "engine": "Perplexity", "claim": "Located on Colfax Avenue",
             "problem": "The adoption center moved in 2024.", "severity": "medium",
             "fix": "Update the address on the Google Business Profile and Yelp, then the website footer."}]},
    }

    # ── Keyword tracking (through the real snapshot code) ────────────────
    seo_kws = client.get("seo_keywords") or []
    comp_domains = [F.domain_of(c["domain"]) for c in comps]
    history = []
    for day, shift in ((14, 4), (7, 2), (0, 0)):
        kr = []
        for i, kw in enumerate(seo_kws):
            pos = [3, 7, 1, 12, None, 5][i % 6]
            pos = pos and max(1, pos + (shift if i % 2 else -(shift // 2)))
            top = ([site] if pos == 1 else comp_domains[:1]) + comp_domains[1:] + ["petfinder.com", "yelp.com", "dumbfriendsleague.example", "coloradopets.example"]
            kr.append({"keyword": kw, "position": pos, "url": f"https://{site}/adopt" if pos else None, "in_local_pack": i == 0,
                       "ai_overview": i % 2 == 0, "ai_overview_cited": i == 2,
                       "competitors": {d: [2, 4, 8, 3, 6, 9][(i + j) % 6] for j, d in enumerate(comp_domains)},
                       "people_also_ask": ["How do I adopt a dog in Denver?"] if i == 0 else [], "volume": [1900, 880, 590, 320, 1300, 210][i % 6],
                       "top_domains": top, "ai_sources": ([site] if i == 2 else []) + ["petfinder.com"], "checked_at": D(day) + "T08:00:00"})
        snap = S.rank_snapshot(kr); snap["date"] = D(day)
        history.append(snap)
    out["seo"] = {"client": client["name"], "website": client.get("website"), "location": F.seo_location(client), "fetched_at": D(0) + "T08:00:00",
                  "keyword_rankings": kr, "rank_history": history,
                  "pagespeed_mobile": {"performance_score": 71, "seo_score": 92, "accessibility": 88, "cwv_pass": True, "lcp": "2.4 s", "cls": "0.04", "tbt": "180 ms"},
                  "pagespeed_desktop": {"performance_score": 93},
                  "search_console": {"clicks": 1840, "impressions": 61200, "ctr": 3.0, "position": 14.2,
                                     "top_keywords": [{"query": q, "clicks": c, "impressions": c * 18, "ctr": 5.5, "position": p} for q, c, p in
                                                      [("maplewood paws", 610, 1.1), ("adopt a dog denver", 190, 4.3), ("dog rescue denver", 150, 6.8),
                                                       ("foster dogs denver", 92, 5.1), ("senior dogs for adoption", 64, 8.9)]],
                                     "top_pages": [{"page": f"https://{site}/adopt", "clicks": 720, "impressions": 21000, "position": 6.2}]},
                  "summary": {"keywords_tracked": len(seo_kws), "keywords_top10": sum(1 for k in kr if k["position"] and k["position"] <= 10)}}

    # ── Organic search ────────────────────────────────────────────────────
    org_kws = [("maplewood paws", 1, 1300, 1.2, 4, 610), ("adopt a dog denver", 4, 1900, 2.4, 31, 180), ("dog rescue denver", 6, 880, 2.1, 28, 64),
               ("foster dogs denver", 5, 320, 1.6, 18, 38), ("senior dogs for adoption", 9, 1300, 1.9, 35, 41), ("rescue dogs near me", 14, 14800, 2.8, 48, 120),
               ("puppy adoption colorado", 17, 2400, 2.2, 40, 22), ("volunteer with dogs denver", 8, 260, 1.1, 15, 18)]
    out["organic"] = {"domain": site, "fetched_at": D(2) + "T06:00:00",
                      "overview": {"keywords": 486, "traffic": 2210, "traffic_value": 3140, "pos_1": 12, "pos_2_3": 26, "pos_4_10": 71,
                                   "pos_11_20": 94, "pos_21_100": 283, "new": 38, "up": 51, "down": 27, "lost": 9},
                      "keywords": [{"keyword": k, "position": p, "change": rnd.choice([None, 2, -1, 3, 0]), "new": False, "volume": v, "cpc": c,
                                    "difficulty": d, "intent": "transactional", "url": f"https://{site}/adopt" if p > 1 else f"https://{site}/", "traffic": t}
                                   for k, p, v, c, d, t in org_kws],
                      "pages": [{"url": f"https://{site}/", "keywords": 41, "traffic": 690, "top_keyword": "maplewood paws"},
                                {"url": f"https://{site}/adopt", "keywords": 88, "traffic": 540, "top_keyword": "adopt a dog denver"},
                                {"url": f"https://{site}/foster", "keywords": 37, "traffic": 160, "top_keyword": "foster dogs denver"}],
                      "competitors": [{"domain": d, "common_keywords": 140 - i * 30, "avg_position": 6.5 + i * 2, "keywords": 3200 - i * 900, "traffic": 9000 - i * 2500}
                                      for i, d in enumerate(comp_domains)],
                      "opportunities": [{"keyword": k, "volume": v, "cpc": 1.4, "difficulty": d, "intent": "commercial"} for k, v, d in
                                        [("dog adoption events denver", 590, 22), ("how to foster a dog", 2400, 34), ("cheap dog adoption denver", 480, 19)]],
                      "authority": None,
                      "history": [{"month": (TODAY.replace(day=1) - datetime.timedelta(days=31 * (5 - i))).strftime("%Y-%m"), "keywords": 380 + i * 21,
                                   "traffic": 1600 + i * 120, "pos_1": 9 + i // 2, "pos_2_3": 20 + i, "pos_4_10": 60 + i * 2} for i in range(6)]}

    # ── Authority ─────────────────────────────────────────────────────────
    refs = [{"domain": f"{n}.example", "authority": a, "links": l, "followed": l - 1, "spam": False, "spam_score": 4,
             "first_seen": D(fs), "lost": None, "traffic": t, "best": a >= 20 and t >= 100}
            for n, a, l, t, fs in [("denverpost", 78, 3, 52000, 400), ("coloradopetguide", 41, 6, 2100, 200), ("milehighmoms", 35, 2, 900, 90),
                                   ("rockymountainvets", 29, 4, 640, 25), ("denverdogparks", 22, 2, 310, 12), ("pawsandpints", 12, 1, 40, 8)]]
    out["authority"] = {"domain": site, "fetched_at": D(5) + "T06:00:00",
                        "summary": {"authority": 31, "backlinks": 2640, "referring_domains": 418, "spam_score": 6, "nofollow_domains": 52},
                        "history": [{"date": (TODAY.replace(day=1) - datetime.timedelta(days=31 * (11 - i))).isoformat(), "authority": 24 + (i * 7) // 11,
                                     "referring_domains": 330 + i * 8, "backlinks": 2100 + i * 50} for i in range(12)],
                        "best_links": [r for r in refs if r["best"]], "links_to_get_back": [{**refs[1], "domain": "coloradogives.example", "lost": D(40)}],
                        "highest": refs, "new_links": [r for r in refs if r["first_seen"] >= D(30)], "lost_links": [],
                        "spam_count": 7, "new_lost_30d": {"new": 14, "lost": 5}, "new_lost_30d_clean": {"new": 3, "lost": 0},
                        "anchors": [{"anchor": a, "sites": s, "share": sh, "links": s * 3} for a, s, sh in
                                    [("maplewood paws", 120, 41.0), (site, 80, 27.3), ("adopt a dog", 34, 11.6), ("click here", 21, 7.2)]],
                        "competitors": [{"domain": site, "name": "You", "you": True, "authority": 31, "referring_domains": 418, "backlinks": 2640}] +
                                       [{"domain": d, "name": c["name"], "you": False, "authority": 38 - i * 6, "referring_domains": 620 - i * 150, "backlinks": 5100 - i * 1300}
                                        for i, (d, c) in enumerate(zip(comp_domains, comps))],
                        "previous": {"authority": 29, "referring_domains": 401, "best_links": 3}}

    # ── Local map (through the real stats code) ──────────────────────────
    lt = client.get("local_tracking") or {}
    names_local = [brand] + [c["name"] for c in comps] + ["Denver Animal Shelter", "Colfax Pet Rescue", "Paws Across Denver"]
    def grid_for(kw_i):
        n = 7; g, pts = [], []
        for y in range(n):
            gr, pr = [], []
            for x in range(n):
                d = math.hypot(x - 3, y - 3)
                rank = None if d > 3.4 and kw_i == 2 else max(1, round(1 + d * (1.1 + kw_i * .5) + rnd.uniform(-.6, .6)))
                order = list(range(1, len(names_local))); rnd.shuffle(order)
                ids = order[:max(0, (rank or 21) - 1)][:19]
                if rank:
                    ids = ids[:rank - 1] + [0] + ids[rank - 1:]
                gr.append(rank); pr.append(ids[:20])
            g.append(gr); pts.append(pr)
        return g, pts
    lat, lng = 39.7392, -104.9903
    points = L.grid_points(lat, lng, 7, 3)
    local_runs = []
    for day in (7, 0):
        kw_out = {}
        for i, kw in enumerate(lt.get("keywords") or []):
            g, pts = grid_for(i)
            businesses = [{"title": nm, "rating": round(rnd.uniform(4.2, 4.9), 1), "reviews": rnd.randint(40, 900), "you": j == 0}
                          for j, nm in enumerate(names_local)]
            kw_out[kw] = {"grid": g, "pts": pts, "businesses": businesses, "center_top3": [businesses[k] for k in pts[3][3][:3]], **L.keyword_stats(g)}
        local_runs.append({"date": D(day), "keywords": kw_out, "center": [lat, lng], "radius_miles": 3, "spacing_miles": 1, "points": points,
                           "cities": {city: {kw: {"map_pack": {"shown": True, "rank": [1, 2, None][i % 3], "top": names_local[:3]},
                                                  "google_maps": {"rank": [1, 3, 7][i % 3], "top": names_local[:5]}}
                                             for i, kw in enumerate(lt.get("keywords") or [])}
                                      for city in (lt.get("cities") or ["Denver, CO"])}})
    out["local"] = {"profile": {"title": brand, "category": "Animal rescue service", "additional_categories": ["Animal shelter"],
                                "address": "1200 Example St, Denver, CO 80205", "phone": "+1 303-555-0142", "url": "https://" + site,
                                "rating": 4.8, "reviews": 386, "is_claimed": True, "place_id": "demo", "lat": lat, "lng": lng,
                                "hours": [{"day": d, "spans": ["11:00 AM–6:00 PM"] if d != "Monday" else []} for d in dows],
                                "checked_at": D(0) + "T07:00:00"},
                    "runs": local_runs}

    # ── Competitor website changes ───────────────────────────────────────
    c0, c1 = (comps + [{"name": "Another rescue", "domain": "another.example"}] * 2)[:2]
    out["watch"] = {"checked_at": D(0) + "T05:00:00",
                    "sites": {F.domain_of(c["domain"]): {"name": c["name"], "checked": D(0), "source": "sitemap", "url_count": 140 + i * 60,
                                                         "pages": {f"https://{F.domain_of(c['domain'])}/": {}}, "baseline": False}
                              for i, c in enumerate(comps)},
                    "changes": [
                        {"date": D(0), "domain": F.domain_of(c0["domain"]), "name": c0["name"], "kind": "new_page",
                         "url": f"https://{F.domain_of(c0['domain'])}/senior-dog-program", "title": "Senior Dog Foster Program",
                         "desc": "Foster a senior dog: we cover all vet care.", "note": f"{c0['name']} launched a senior-dog foster program, a new way to recruit fosters."},
                        {"date": D(0), "domain": F.domain_of(c1["domain"]), "name": c1["name"], "kind": "changed",
                         "url": f"https://{F.domain_of(c1['domain'])}/", "title": c1["name"],
                         "diff": {"h1": {"from": "Adopt a rescue dog", "to": "Double your gift this month"},
                                  "added": ["A local donor will match every gift up to $10,000 through the end of the month."],
                                  "added_n": 1, "removed_n": 0, "words": {"from": 420, "to": 455}},
                         "note": f"{c1['name']} is running a matching-gift campaign on its homepage."},
                        {"date": D(7), "domain": F.domain_of(c0["domain"]), "name": c0["name"], "kind": "new_page",
                         "url": f"https://{F.domain_of(c0['domain'])}/events/adoption-day", "title": "Saturday Adoption Day", "desc": "Meet 30 adoptable dogs."}]}
    return out
