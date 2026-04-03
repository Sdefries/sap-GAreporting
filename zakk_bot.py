"""
zakk_bot.py
─────────────────────────────────────────────────────────────────────────────
Zakk Bot — SAP's automated campaign builder.

Reads a campaign request submission (from the client portal), generates
ad copy using Claude AI, uploads assets from Google Drive, builds the full
campaign structure via Google Ads API, and posts a preview to Slack for
one-click approval before anything goes live.

WORKFLOW
  1. Receive submission (from Web3Forms webhook or manual trigger)
  2. Load client profile from clients.json
  3. Generate headlines, descriptions, keywords via Claude API
  4. Download assets from Google Drive folder
  5. Upload assets to Google Ads asset library
  6. Build campaign structure (campaigns, ad groups, keywords)
  7. Save as DRAFT — nothing goes live yet
  8. Post full preview to Slack for approval
  9. On approval → push live via approve_campaign.py

USAGE
  python zakk_bot.py --submission submission.json   # process a submission file
  python zakk_bot.py --test --slug pup-profile      # test run with fake data
  python zakk_bot.py --approve CUSTOMER_ID CAMPAIGN_ID  # approve and go live

ENV VARS REQUIRED
  GOOGLE_ADS_YAML        — Google Ads API credentials
  ANTHROPIC_API_KEY      — Claude API key for ad copy generation
  SLACK_WEBHOOK          — Slack webhook for approval notifications
  GOOGLE_DRIVE_API_KEY   — Google Drive API key for asset downloads (optional)
"""

import json
import os
import sys
import datetime
import argparse
import urllib.request
import urllib.parse
import tempfile

# ── LOAD CLIENTS ──────────────────────────────────────────────────────────

with open("clients.json") as f:
    CLIENTS = {c["slug"]: c for c in json.load(f)}

SLACK_WEBHOOK     = os.environ.get("SLACK_WEBHOOK", "")

# Load portfolio intelligence — feeds into Claude copy generation
_intel_path = "ad_intelligence.json"
AD_INTELLIGENCE = {}
if os.path.exists(_intel_path):
    with open(_intel_path) as f:
        AD_INTELLIGENCE = json.load(f)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_ADS_YAML   = os.environ.get("GOOGLE_ADS_YAML", "")

# ── CLAUDE AD COPY GENERATOR ──────────────────────────────────────────────

def generate_ad_copy(client, submission):
    """
    Call Claude API to generate headlines, descriptions, and keywords
    based on the client's mission and the campaign submission.
    """
    if not ANTHROPIC_API_KEY:
        print("  No ANTHROPIC_API_KEY — using placeholder copy")
        return _placeholder_copy(client, submission)

    name      = client["name"]
    org_type   = client.get("org_type", "nonprofit")
    org_model  = client.get("org_model", "location_based")
    animal_type = client.get("animal_type", None)
    
    # Pull org model intelligence
    org_model_intel = AD_INTELLIGENCE.get("org_models", {}).get(org_model, {})
    animal_intel = AD_INTELLIGENCE.get("org_models", {}).get(
        f"{animal_type}_rescue" if animal_type else org_model, {}
    )
    org_copy_principles = "\n".join(["- " + p for p in org_model_intel.get("copy_principles", [])])
    org_specific_copy = "\n".join(org_model_intel.get(f"{animal_type}_specific_copy" if animal_type else "copy_principles", [])[:5])
    org_keywords_include = ", ".join(org_model_intel.get("keywords", {}).get("include", [])[:8])
    org_keywords_exclude = ", ".join(org_model_intel.get("keywords", {}).get("exclude", [])[:5])
    mission   = client.get("keywords", {}).get("include_themes", [])
    negatives = client.get("keywords", {}).get("negative", [])
    camp_type = submission.get("campaign_type", "Donate")
    goal      = submission.get("goal", "")
    url       = submission.get("landing_url", "")

    prompt = f"""You are a world-class Google Ads copywriter with deep expertise in nonprofit advertising and donor psychology.

CLIENT: {name}
ORG TYPE: {org_type}
CAMPAIGN TYPE: {camp_type}
GOAL: {goal}
LANDING PAGE: {url}

PORTFOLIO INTELLIGENCE — WHAT WE KNOW WORKS ACROSS OUR 15 NONPROFIT CLIENTS:

Top CTR headline themes (use these as inspiration):
{high_ctr_themes}

High-converting description themes:
{high_conv_descs}

For {camp_type} campaigns specifically:
{camp_direction}

Landing page context:
{camp_landing}

Lessons from top performers in this org type:
{winner_lessons}

Apply these real-world patterns from our portfolio when writing copy.

MISSION THEMES: {', '.join(mission)}
ORG MODEL: {org_model} ({'Has a physical location — shelters, ranch, facility. Drives visits.' if org_model == 'location_based' else 'Foster network — no facility. Animals live in foster homes. Foster recruitment is the #1 priority.'})
ANIMAL TYPE: {animal_type or 'n/a'}

ORG MODEL COPY RULES — FOLLOW THESE EXACTLY:
{org_copy_principles}

ORG-SPECIFIC COPY EXAMPLES THAT WORK FOR THIS TYPE:
{org_specific_copy}

KEYWORDS TO PRIORITIZE FOR THIS ORG MODEL:
{org_keywords_include}

KEYWORDS TO NEVER INCLUDE FOR THIS ORG MODEL:
{org_keywords_exclude}
WORDS TO NEVER USE: {', '.join(negatives)}

YOUR JOB: Write the highest-converting Google Ads copy possible for this nonprofit campaign.

WHAT DRIVES CONVERSIONS IN NONPROFIT ADS (apply these principles):

1. SPECIFICITY WINS — "12 Dogs Need Homes This Week" outperforms "Adopt a Dog Today"
   Use real numbers, timeframes, and specifics whenever possible from the goal description.

2. OUTCOME-FIRST LANGUAGE — Lead with what the person achieves, not what they do.
   "Save a Life Today" beats "Complete an Application"
   "Give a Dog a Second Chance" beats "Submit a Donation"

3. URGENCY THAT'S REAL — Only use urgency if the goal mentions it. If it does, make it specific.
   "3 Days Left to Help" beats "Act Now"

4. LOCAL SIGNAL — If a city or region is mentioned in the goal, use it in at least 2 headlines.
   "LA Dogs Need You Now" converts better than the same line without location.

5. QUESTION HEADLINES — Pull people in with the right question.
   "Could You Foster One Dog?" "Ready to Save a Life?" "Your Help Needed Today?"

6. SOCIAL PROOF WHEN AVAILABLE — If any numbers exist in the goal, use them.
   "Join 2,400 Supporters" "347 Rescues and Counting"

7. MISSION TRANSPARENCY — Donors respond to honesty about what their money does.
   "100% Goes to Dog Care" "Every Dollar Saves a Shelter Dog"

8. SHORT POWER HEADLINES — Always include 3-4 headlines under 20 characters.
   Google pins short headlines in prime positions. These often have the highest CTR.
   Examples: "Adopt Today", "Save a Dog", "Foster Now", "Help Today"

9. KEYWORD ANCHORING — Include the campaign type keyword naturally in at least 3 headlines.
   If campaign type is Donate: "Donate to Save Dogs", "Your Donation Saves Lives"
   If campaign type is Adopt: "Adopt a Rescue Dog", "Adoption Open Now"

10. CTA VARIATION — Vary the call to action across headlines.
    Use: Adopt, Foster, Donate, Help, Save, Give, Join, Act, Support, Change
    Never repeat the same CTA more than twice.

HEADLINE RULES (STRICT):
- Max 30 characters each (count carefully — this is enforced)
- Write exactly 15 headlines
- At least 3 must be under 20 characters
- NO exclamation marks (Google Ad Grants policy violation)
- NO superlatives: best, #1, greatest, most amazing, world-class
- NO ALL CAPS
- Include the org name or mission in at least 2 headlines
- Vary length: mix short punchy and longer descriptive

DESCRIPTION RULES (STRICT):
- Max 90 characters each (count carefully)
- Write exactly 4 descriptions
- Each must be a complete thought that can stand alone
- Include a clear call to action in each
- At least one should mention the specific goal or urgency from the submission

KEYWORD STRATEGY:
- Write 20 keywords — mix of 1-word, 2-word, and 3-word phrases
- Include both branded (org name variations) and generic (mission-based) terms
- Think like someone searching in a moment of intent: "dogs need homes near me", "how to help animals"
- Weight toward informational and intent-based queries

NEGATIVE KEYWORDS:
- Write 15 negative keywords
- Block: commercial intent (buy, sell, price, cost, free), competitor confusion, job seekers, irrelevant verticals
- Specific to this org type: {org_type}

Respond ONLY with valid JSON, no preamble, no markdown fences:
{{
  "headlines": ["headline 1", ... exactly 15],
  "descriptions": ["description 1", "description 2", "description 3", "description 4"],
  "keywords": ["keyword 1", ... exactly 20],
  "negative_keywords": ["neg 1", ... exactly 15]
}}
"""

    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01"
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            text = data["content"][0]["text"]
            # Strip any markdown fences
            text = text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            return json.loads(text.strip())
    except Exception as e:
        print(f"  Claude API error: {e}")
        return _placeholder_copy(client, submission)


def _placeholder_copy(client, submission):
    name = client["name"]
    camp_type = submission.get("campaign_type", "Donate")
    return {
        "headlines": [
            f"Support {name}",
            f"Help Animals in Need",
            f"Make a Difference Today",
            f"Your Support Saves Lives",
            f"Adopt Don't Shop",
            f"Give Animals a Home",
            f"Every Dollar Counts",
            f"Change a Life Today",
            f"Animals Need Your Help",
            f"Be a Hero for Pets",
            f"Save a Life Today",
            f"Animals Are Waiting",
            f"Join Our Mission",
            f"Help Us Help Them",
            f"Make an Impact Now"
        ],
        "descriptions": [
            f"{name} is dedicated to helping animals in need. Your support makes rescue possible.",
            f"Every dollar goes directly to animal care. Donate today and save a life.",
            f"Join thousands of supporters making a difference. Your gift matters.",
            f"Help us continue our mission. {camp_type} today and change an animal's life."
        ],
        "keywords": [
            "animal rescue donation", "donate to animal shelter", "help animals",
            "animal charity", "pet rescue organization", "support animal rescue",
            "animal welfare donation", "rescue animals near me", "adopt a pet",
            "animal rescue nonprofit", "dog rescue donation", "cat rescue charity",
            "pet adoption", "animal shelter support", "save animals",
            "rescue dog adoption", "animal rescue fund", "pet charity donation",
            "foster animals", "animal rescue volunteer"
        ],
        "negative_keywords": [
            "jobs", "careers", "salary", "breeders", "buy a dog",
            "puppy mill", "pet store", "how much", "free", "discount"
        ]
    }


# ── CAMPAIGN STRUCTURE BUILDER ────────────────────────────────────────────

def build_campaign_structure(client, submission, copy_data):
    """
    Build the complete campaign structure ready for Google Ads API.
    Returns a dict describing what will be created.
    """
    camp_type   = submission.get("campaign_type", "Donate")
    landing_url = submission.get("landing_url", client.get("website", ""))
    urgency     = submission.get("urgency", "normal")
    geo         = client.get("geo", {})
    budget      = 50.00  # Default daily budget — $50/day = ~$1,500/month

    # Campaign name with date prefix for easy sorting
    date_prefix  = datetime.date.today().strftime("%Y-%m")
    campaign_name = f"{date_prefix} {camp_type} — {client['name']}"

    structure = {
        "campaign": {
            "name":              campaign_name,
            "status":            "PAUSED",  # Always starts paused for review
            "advertising_channel_type": "SEARCH",
            "bidding_strategy":  "TARGET_CPA",
            "target_cpa":        10.00,
            "budget_micros":     int(budget * 1_000_000),
            "start_date":        datetime.date.today().strftime("%Y%m%d"),
            "geo_targets":       geo.get("locations", []),
            "geo_radius_miles":  geo.get("radius_miles"),
        },
        "ad_group": {
            "name":    f"{camp_type} — Main",
            "status":  "PAUSED",
            "cpc_bid": 2.00,
        },
        "ads": [{
            "type":         "RESPONSIVE_SEARCH_AD",
            "headlines":    copy_data["headlines"],
            "descriptions": copy_data["descriptions"],
            "final_url":    landing_url,
        }],
        "keywords": [
            {"text": kw, "match_type": "BROAD"}
            for kw in copy_data["keywords"]
        ],
        "negative_keywords": [
            {"text": kw, "match_type": "BROAD"}
            for kw in copy_data["negative_keywords"]
        ],
        "meta": {
            "client":         client["name"],
            "slug":           client["slug"],
            "campaign_type":  camp_type,
            "goal":           submission.get("goal", ""),
            "urgency":        urgency,
            "drive_url":      submission.get("drive_url", ""),
            "youtube_url":    submission.get("youtube_url", ""),
            "submitted_at":   submission.get("submitted_at", ""),
            "built_at":       datetime.datetime.now().isoformat(),
            "status":         "PENDING_APPROVAL",
        }
    }

    return structure


# ── SLACK APPROVAL MESSAGE ────────────────────────────────────────────────

def post_approval_request(client, submission, structure, copy_data, dry_run=False):
    """
    Post the full campaign preview to Slack for approval.
    Includes all headlines, descriptions, keywords, and campaign settings.
    """
    name      = client["name"]
    camp_type = submission.get("campaign_type", "Donate")
    goal      = submission.get("goal", "")[:200]
    urgency   = submission.get("urgency", "normal")
    drive_url = submission.get("drive_url", "")
    yt_url    = submission.get("youtube_url", "")
    camp      = structure["campaign"]

    urgency_flag = "🔥 RUSH — needs same day review" if urgency == "rush" else "⏱ Standard — 24 hours"

    headlines_str    = "\n".join([f"  {i+1}. {h}" for i, h in enumerate(copy_data["headlines"])])
    descriptions_str = "\n".join([f"  {i+1}. {d}" for i, d in enumerate(copy_data["descriptions"])])
    keywords_str     = ", ".join(copy_data["keywords"][:10]) + "..."
    negatives_str    = ", ".join(copy_data["negative_keywords"][:5]) + "..."

    msg = f"""🤖 *Zakk Bot — New Campaign Ready for Review*

*Client:* {name}
*Campaign type:* {camp_type}
*Urgency:* {urgency_flag}

*Goal:*
> {goal}

*Landing page:* {structure['ads'][0]['final_url']}
*Daily budget:* ${camp['budget_micros'] / 1_000_000:.0f}
*Geo targets:* {', '.join(camp['geo_targets']) if camp['geo_targets'] else 'National'}

*Headlines ({len(copy_data['headlines'])}):*
```
{headlines_str}
```

*Descriptions ({len(copy_data['descriptions'])}):*
```
{descriptions_str}
```

*Keywords (showing 10 of {len(copy_data['keywords'])}):**
{keywords_str}

*Negative keywords (showing 5 of {len(copy_data['negative_keywords'])}):**
{negatives_str}

{f'*Assets:* Drive folder linked ✓' if drive_url else '*Assets:* No Drive folder provided'}
{f'*YouTube:* {yt_url}' if yt_url else ''}

*Status:* Campaign built as DRAFT — nothing is live yet.

To approve and push live, run:
`python zakk_bot.py --approve {client.get("google_ads_id", "ACCOUNT_ID")} CAMPAIGN_ID`

Or reply here with *approve* and the campaign ID."""

    if dry_run:
        print(f"\n[DRY RUN — Slack message would be:]")
        print(msg)
        return

    if not SLACK_WEBHOOK:
        print("  No SLACK_WEBHOOK — skipping Slack notification")
        return

    payload = json.dumps({"text": msg}).encode()
    req = urllib.request.Request(
        SLACK_WEBHOOK, data=payload,
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print("  Slack approval request sent to #google-ads")
    except Exception as e:
        print(f"  Slack error: {e}")


# ── GOOGLE ADS CAMPAIGN BUILDER ───────────────────────────────────────────

def push_to_google_ads(client, structure, dry_run=False):
    """
    Push the campaign structure to Google Ads API as a PAUSED draft.
    Returns the campaign ID if successful.
    """
    if dry_run:
        print("  [DRY RUN] Would create campaign in Google Ads:")
        print(f"    Name: {structure['campaign']['name']}")
        print(f"    Budget: ${structure['campaign']['budget_micros'] / 1_000_000:.0f}/day")
        print(f"    Ad group: {structure['ad_group']['name']}")
        print(f"    Keywords: {len(structure['keywords'])}")
        print(f"    Negative keywords: {len(structure['negative_keywords'])}")
        return "DRY_RUN_CAMPAIGN_ID"

    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException
    except ImportError:
        print("  google-ads package not installed — skipping Google Ads push")
        print("  Run: pip install google-ads")
        return None

    if not os.path.exists("google-ads.yaml"):
        print("  google-ads.yaml not found — skipping Google Ads push")
        return None

    try:
        ga_client   = GoogleAdsClient.load_from_storage("google-ads.yaml")
        customer_id = client.get("google_ads_id", "").replace("-", "")

        # ── Create budget ──
        budget_service = ga_client.get_service("CampaignBudgetService")
        budget_op      = ga_client.get_type("CampaignBudgetOperation")
        budget         = budget_op.create
        budget.name    = f"{structure['campaign']['name']} Budget"
        budget.amount_micros = structure["campaign"]["budget_micros"]
        budget.delivery_method = ga_client.enums.BudgetDeliveryMethodEnum.STANDARD

        budget_resp = budget_service.mutate_campaign_budgets(
            customer_id=customer_id, operations=[budget_op]
        )
        budget_resource = budget_resp.results[0].resource_name
        print(f"  Budget created: {budget_resource}")

        # ── Create campaign ──
        camp_service = ga_client.get_service("CampaignService")
        camp_op      = ga_client.get_type("CampaignOperation")
        camp         = camp_op.create
        camp.name    = structure["campaign"]["name"]
        camp.status  = ga_client.enums.CampaignStatusEnum.PAUSED
        camp.advertising_channel_type = ga_client.enums.AdvertisingChannelTypeEnum.SEARCH
        camp.campaign_budget = budget_resource
        camp.target_cpa.target_cpa_micros = int(structure["campaign"]["target_cpa"] * 1_000_000)

        camp_resp    = camp_service.mutate_campaigns(customer_id=customer_id, operations=[camp_op])
        camp_resource = camp_resp.results[0].resource_name
        camp_id      = camp_resource.split("/")[-1]
        print(f"  Campaign created (PAUSED): {camp_resource}")

        # ── Create ad group ──
        ag_service = ga_client.get_service("AdGroupService")
        ag_op      = ga_client.get_type("AdGroupOperation")
        ag         = ag_op.create
        ag.name    = structure["ad_group"]["name"]
        ag.status  = ga_client.enums.AdGroupStatusEnum.PAUSED
        ag.campaign = camp_resource
        ag.cpc_bid_micros = int(structure["ad_group"]["cpc_bid"] * 1_000_000)

        ag_resp      = ag_service.mutate_ad_groups(customer_id=customer_id, operations=[ag_op])
        ag_resource  = ag_resp.results[0].resource_name
        print(f"  Ad group created (PAUSED): {ag_resource}")

        # ── Create responsive search ad ──
        ad_service = ga_client.get_service("AdGroupAdService")
        ad_op      = ga_client.get_type("AdGroupAdOperation")
        ad_group_ad = ad_op.create
        ad_group_ad.ad_group = ag_resource
        ad_group_ad.status = ga_client.enums.AdGroupAdStatusEnum.PAUSED

        rsa = ad_group_ad.ad.responsive_search_ad
        for i, headline in enumerate(structure["ads"][0]["headlines"]):
            asset = ga_client.get_type("AdTextAsset")
            asset.text = headline[:30]  # enforce character limit
            rsa.headlines.append(asset)

        for desc in structure["ads"][0]["descriptions"]:
            asset = ga_client.get_type("AdTextAsset")
            asset.text = desc[:90]  # enforce character limit
            rsa.descriptions.append(asset)

        rsa.final_urls.append(structure["ads"][0]["final_url"])

        ad_service.mutate_ad_group_ads(customer_id=customer_id, operations=[ad_op])
        print(f"  Responsive search ad created (PAUSED)")

        # ── Add keywords ──
        kw_service = ga_client.get_service("AdGroupCriterionService")
        kw_ops     = []
        for kw in structure["keywords"]:
            op = ga_client.get_type("AdGroupCriterionOperation")
            criterion = op.create
            criterion.ad_group = ag_resource
            criterion.status = ga_client.enums.AdGroupCriterionStatusEnum.ENABLED
            criterion.keyword.text = kw["text"]
            criterion.keyword.match_type = ga_client.enums.KeywordMatchTypeEnum.BROAD
            kw_ops.append(op)

        if kw_ops:
            kw_service.mutate_ad_group_criteria(customer_id=customer_id, operations=kw_ops)
            print(f"  {len(kw_ops)} keywords added")

        # ── Add negative keywords ──
        neg_ops = []
        for kw in structure["negative_keywords"]:
            op = ga_client.get_type("CampaignCriterionOperation")
            criterion = op.create
            criterion.campaign = camp_resource
            criterion.negative = True
            criterion.keyword.text = kw["text"]
            criterion.keyword.match_type = ga_client.enums.KeywordMatchTypeEnum.BROAD
            neg_ops.append(op)

        if neg_ops:
            neg_service = ga_client.get_service("CampaignCriterionService")
            neg_service.mutate_campaign_criteria(customer_id=customer_id, operations=neg_ops)
            print(f"  {len(neg_ops)} negative keywords added")

        return camp_id

    except Exception as e:
        print(f"  Google Ads error: {e}")
        return None


# ── SAVE STRUCTURE FOR APPROVAL ───────────────────────────────────────────

def save_pending(slug, structure, copy_data):
    """Save campaign structure to pending/ folder for approval tracking."""
    os.makedirs("pending", exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"pending/{slug}_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump({
            "structure": structure,
            "copy_data": copy_data,
        }, f, indent=2)
    print(f"  Saved pending campaign: {filename}")
    return filename


# ── MAIN ──────────────────────────────────────────────────────────────────

def run(submission, dry_run=False):
    slug = submission.get("client_slug", "")
    if not slug or slug not in CLIENTS:
        # Try to find by name
        name = submission.get("client_name", "")
        slug = next((s for s, c in CLIENTS.items() if c["name"] == name), None)
        if not slug:
            print(f"ERROR: Client not found in clients.json: {submission.get('client_name', submission.get('client_slug', 'unknown'))}")
            sys.exit(1)

    client = CLIENTS[slug]
    print(f"\n🤖 Zakk Bot — Processing campaign request")
    print(f"   Client: {client['name']}")
    print(f"   Type:   {submission.get('campaign_type', '?')}")
    print(f"   Urgency: {submission.get('urgency', 'normal')}")
    print(f"   Mode:   {'DRY RUN' if dry_run else 'LIVE'}\n")

    # Step 1: Generate ad copy
    print("Step 1: Generating ad copy via Claude AI...")
    copy_pref = submission.get("copy_preference", "ai")
    if copy_pref == "manual" and submission.get("manual_copy", "").strip():
        print("  Using manual copy provided by client")
        lines = submission["manual_copy"].strip().split("\n")
        copy_data = {
            "headlines":        [l.strip() for l in lines if l.strip() and l.strip() != "---"][:15],
            "descriptions":     [l.strip() for l in lines if l.strip() and l.strip() != "---"][15:19],
            "keywords":         [],
            "negative_keywords": [],
        }
        # Still generate keywords even if copy is manual
        ai_data = generate_ad_copy(client, submission)
        copy_data["keywords"]          = ai_data["keywords"]
        copy_data["negative_keywords"] = ai_data["negative_keywords"]
    else:
        copy_data = generate_ad_copy(client, submission)

    print(f"  Generated {len(copy_data['headlines'])} headlines, {len(copy_data['descriptions'])} descriptions")
    print(f"  Generated {len(copy_data['keywords'])} keywords, {len(copy_data['negative_keywords'])} negatives")

    # Step 2: Build campaign structure
    print("\nStep 2: Building campaign structure...")
    structure = build_campaign_structure(client, submission, copy_data)
    print(f"  Campaign: {structure['campaign']['name']}")
    print(f"  Status:   PAUSED (awaiting approval)")

    # Step 3: Push to Google Ads as draft
    print("\nStep 3: Creating campaign in Google Ads (PAUSED)...")
    campaign_id = push_to_google_ads(client, structure, dry_run=dry_run)
    if campaign_id:
        structure["meta"]["google_ads_campaign_id"] = campaign_id

    # Step 4: Save pending for approval
    print("\nStep 4: Saving pending approval record...")
    pending_file = save_pending(slug, structure, copy_data)

    # Step 5: Post to Slack for approval
    print("\nStep 5: Posting approval request to Slack...")
    post_approval_request(client, submission, structure, copy_data, dry_run=dry_run)

    print(f"\n✓ Done — campaign is PAUSED and awaiting approval")
    print(f"  Pending file: {pending_file}")
    if campaign_id and campaign_id != "DRY_RUN_CAMPAIGN_ID":
        print(f"  Campaign ID: {campaign_id}")
        print(f"  Approve with: python zakk_bot.py --approve {client.get('google_ads_id', 'ACCOUNT_ID')} {campaign_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zakk Bot — SAP Campaign Builder")
    parser.add_argument("--submission", help="Path to submission JSON file")
    parser.add_argument("--test",       action="store_true", help="Run with test data")
    parser.add_argument("--slug",       help="Client slug for test mode")
    parser.add_argument("--dry-run",    action="store_true", help="Don't push to Google Ads or Slack")
    parser.add_argument("--approve",    nargs=2, metavar=("CUSTOMER_ID", "CAMPAIGN_ID"), help="Approve and enable a paused campaign")
    args = parser.parse_args()

    if args.approve:
        # Approval mode — enable a paused campaign
        customer_id, campaign_id = args.approve
        print(f"Approving campaign {campaign_id} for account {customer_id}...")
        try:
            from google.ads.googleads.client import GoogleAdsClient
            ga_client = GoogleAdsClient.load_from_storage("google-ads.yaml")
            camp_service = ga_client.get_service("CampaignService")
            op = ga_client.get_type("CampaignOperation")
            camp = op.update
            camp.resource_name = f"customers/{customer_id}/campaigns/{campaign_id}"
            camp.status = ga_client.enums.CampaignStatusEnum.ENABLED
            from google.protobuf import field_mask_pb2
            op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=["status"]))
            camp_service.mutate_campaigns(customer_id=customer_id, operations=[op])
            print(f"✓ Campaign {campaign_id} is now LIVE")
        except Exception as e:
            print(f"Error approving campaign: {e}")
        sys.exit(0)

    if args.test:
        slug = args.slug or "pup-profile"
        client = CLIENTS.get(slug, list(CLIENTS.values())[0])
        test_submission = {
            "client_name":     client["name"],
            "client_slug":     slug,
            "campaign_type":   "Donate",
            "goal":            "Spring fundraiser — we need donations to cover medical costs for 8 dogs rescued this week.",
            "landing_url":     client.get("website", "https://example.org"),
            "copy_preference": "ai",
            "urgency":         "normal",
            "notes":           "Test submission from Zakk Bot",
            "drive_url":       "",
            "youtube_url":     "",
            "submitted_at":    datetime.datetime.now().isoformat(),
        }
        run(test_submission, dry_run=args.dry_run)

    elif args.submission:
        with open(args.submission) as f:
            submission = json.load(f)
        run(submission, dry_run=args.dry_run)

    else:
        parser.print_help()
