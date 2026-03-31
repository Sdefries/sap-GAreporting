"""
generate_ad_copy.py
────────────────────
Uses OpenAI to generate nonprofit-optimized ad copy, then pushes it
directly to Google Ads as a new Responsive Search Ad.

Why this matters: manually writing RSAs for 14 clients with 15 headlines
and 4 descriptions each = 266 pieces of copy. This script does it in seconds.

Usage:
  # Generate and push ad copy for one client
  python generate_ad_copy.py \
    --client city-dogs-kitties \
    --campaign "Dog Adoption 2026" \
    --mission "We rescue dogs and cats from high-kill shelters in the DC metro area" \
    --url https://citydogsandkitties.org/adopt \
    --goal "drive adoption applications"

  # Preview only — don't push to Google Ads
  python generate_ad_copy.py --client scienceworks --mission "..." --url "..." --preview
"""

import argparse
import json
import os
from openai import OpenAI
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

with open("clients.json") as f:
    CLIENTS = {c["slug"]: c for c in json.load(f)}

openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def generate_copy(mission: str, goal: str, org_name: str) -> dict:
    """Use GPT-4 to generate RSA headlines and descriptions."""
    prompt = f"""You are a Google Ads copywriter specializing in nonprofit Ad Grants campaigns.

Organization: {org_name}
Mission: {mission}
Campaign goal: {goal}

Write ad copy that:
- Is emotionally compelling and action-oriented
- Complies with Google Ad Grants policies (no commercial tone)
- Highlights nonprofit credibility (501c3, free services, community impact)
- Uses clear calls to action

Respond with ONLY valid JSON in this exact format:
{{
  "headlines": [
    "headline 1 (max 30 chars)",
    "headline 2 (max 30 chars)",
    ... 8 headlines total
  ],
  "descriptions": [
    "description 1 (max 90 chars)",
    "description 2 (max 90 chars)"
  ]
}}

Rules:
- Each headline MUST be 30 characters or fewer
- Each description MUST be 90 characters or fewer
- Do not use exclamation marks in headlines (Google policy)
- Do not use ALL CAPS
- Vary the messaging — don't repeat the same idea
"""

    response = openai_client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.7,
    )

    copy = json.loads(response.choices[0].message.content)

    # Enforce character limits
    copy["headlines"] = [h[:30] for h in copy["headlines"]]
    copy["descriptions"] = [d[:90] for d in copy["descriptions"]]

    return copy


def find_ad_group(ads_client, customer_id: str, campaign_name: str) -> str | None:
    """Find the first enabled ad group in a campaign."""
    ga_service = ads_client.get_service("GoogleAdsService")
    query = f"""
        SELECT ad_group.resource_name
        FROM ad_group
        WHERE campaign.name LIKE '%{campaign_name}%'
          AND ad_group.status = 'ENABLED'
        LIMIT 1
    """
    response = ads_client.get_service("GoogleAdsService").search(
        customer_id=customer_id, query=query
    )
    rows = list(response)
    return rows[0].ad_group.resource_name if rows else None


def push_rsa(ads_client, customer_id: str, ag_rn: str,
             headlines: list[str], descriptions: list[str], final_url: str) -> str:
    """Push the generated copy to Google Ads as a Responsive Search Ad."""
    ad_service = ads_client.get_service("AdGroupAdService")
    op = ads_client.get_type("AdGroupAdOperation")
    aga = op.create

    aga.status = ads_client.enums.AdGroupAdStatusEnum.PAUSED  # Review before enabling
    aga.ad_group = ag_rn

    rsa = aga.ad.responsive_search_ad
    for h in headlines:
        asset = ads_client.get_type("AdTextAsset")
        asset.text = h
        rsa.headlines.append(asset)
    for d in descriptions:
        asset = ads_client.get_type("AdTextAsset")
        asset.text = d
        rsa.descriptions.append(asset)

    aga.ad.final_urls.append(final_url)

    response = ad_service.mutate_ad_group_ads(customer_id=customer_id, operations=[op])
    return response.results[0].resource_name


def print_copy_preview(org_name: str, copy: dict) -> None:
    print(f"\n  Generated ad copy for {org_name}")
    print(f"  {'─'*50}")
    print(f"  Headlines ({len(copy['headlines'])}):")
    for i, h in enumerate(copy["headlines"], 1):
        chars = len(h)
        flag = " ⚠ OVER LIMIT" if chars > 30 else ""
        print(f"    {i:2}. [{chars:2}/30] {h}{flag}")
    print(f"\n  Descriptions ({len(copy['descriptions'])}):")
    for i, d in enumerate(copy["descriptions"], 1):
        chars = len(d)
        flag = " ⚠ OVER LIMIT" if chars > 90 else ""
        print(f"    {i}. [{chars:2}/90] {d}{flag}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI-generated ad copy for nonprofit clients")
    parser.add_argument("--client",   required=True, help="Client slug")
    parser.add_argument("--campaign", help="Campaign name to add the RSA to")
    parser.add_argument("--mission",  required=True, help="Organization mission statement")
    parser.add_argument("--url",      required=True, help="Final URL for the ad")
    parser.add_argument("--goal",     default="drive website traffic and awareness",
                        help="Campaign goal (e.g. 'drive adoptions', 'recruit volunteers')")
    parser.add_argument("--preview",  action="store_true",
                        help="Print copy only — do not push to Google Ads")
    args = parser.parse_args()

    client_data = CLIENTS.get(args.client)
    if not client_data:
        print(f"Client '{args.client}' not found in clients.json")
        exit(1)

    org_name = client_data["name"]
    customer_id = client_data["google_ads_id"].replace("-", "")

    print(f"Generating ad copy for {org_name}...")
    copy = generate_copy(args.mission, args.goal, org_name)
    print_copy_preview(org_name, copy)

    if args.preview:
        print("\n  Preview mode — not pushed to Google Ads")
        exit(0)

    if not args.campaign:
        print("\n  --campaign required to push to Google Ads")
        exit(1)

    print(f"\n  Pushing to Google Ads (campaign: {args.campaign})...")
    ads_client = GoogleAdsClient.load_from_storage("google-ads.yaml", version="v16")

    try:
        ag_rn = find_ad_group(ads_client, customer_id, args.campaign)
        if not ag_rn:
            print(f"  No enabled ad group found in campaign '{args.campaign}'")
            exit(1)

        rn = push_rsa(ads_client, customer_id, ag_rn,
                      copy["headlines"], copy["descriptions"], args.url)
        print(f"  RSA created (PAUSED — review before enabling): {rn}")
        print(f"  View at: https://ads.google.com/aw/ads?customerId={customer_id}")

    except GoogleAdsException as ex:
        for error in ex.failure.errors:
            print(f"  API error: {error.message}")
