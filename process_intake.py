#!/usr/bin/env python3
"""
process_intake.py - AI Campaign Generation Engine for Google Ad Grants

This script transforms nonprofit intake form submissions into fully-structured
Google Ads campaigns using Claude AI with expert-level Google Ads strategy.

Features:
- Website scraping for additional context
- Org-type-specific campaign strategies
- Google Ad Grants compliance baked in
- RSA best practices (15 headlines, 4 descriptions)
- Intelligent keyword generation with match types
- Negative keyword recommendations
- Quality Score optimization signals

Author: SponsorAPurpose
"""

import os
import sys
import json
import re
import hashlib
from datetime import datetime
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
from anthropic import Anthropic

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK")
DRAFTS_DIR = "drafts"

# Scraping config
MAX_PAGES_TO_SCRAPE = 8
SCRAPE_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (compatible; SAPBot/1.0; +https://sponsorapurpose.org)"

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE AD GRANTS COMPLIANCE RULES
# ═══════════════════════════════════════════════════════════════════════════════

AD_GRANTS_RULES = """
## GOOGLE AD GRANTS COMPLIANCE REQUIREMENTS (CRITICAL)

You MUST follow these rules or the account will be suspended:

### Account Structure
- Minimum 2 ad groups per campaign
- Minimum 2 active ads per ad group
- Campaigns must have clear, distinct themes

### Keyword Rules
- NO single-word keywords (except: brand name, recognized medical conditions, recognized abbreviations)
- NO overly generic keywords (e.g., "free videos", "today's news", "easy yoga")
- All keywords must maintain Quality Score of 3 or higher
- Use phrase match [" "] and exact match [ ] primarily
- Broad match only acceptable with smart bidding strategies

### Performance Requirements
- Account must maintain 5% CTR minimum (account-wide average)
- Keywords below 5% CTR for 2 consecutive months should be paused
- Must have valid conversion tracking with at least 1 conversion/month

### Bid Constraints
- $2.00 USD max CPC unless using Maximize Conversions bidding
- Recommend Maximize Conversions or Target CPA bidding to bypass $2 cap

### Prohibited Content
- No financial products/services ads
- No asking for donations of cars, boats, property (can accept, can't solicit via ads)
"""

# ═══════════════════════════════════════════════════════════════════════════════
# EXPERT CAMPAIGN STRATEGY
# ═══════════════════════════════════════════════════════════════════════════════

CAMPAIGN_STRATEGY = """
## EXPERT GOOGLE ADS CAMPAIGN ARCHITECTURE

### Campaign Types to Consider (select based on org type and goals):

1. **BRAND CAMPAIGN** (Always include)
   - Org name + variations + misspellings
   - Protects brand, highest CTR, lowest cost
   - Example: "flathead animal shelter", "flathead humane society"

2. **SERVICE/PROGRAM CAMPAIGNS** (Core campaigns)
   - One campaign per major service category
   - Tightly themed ad groups within each
   - Example: "Pet Adoption" campaign with ad groups for "dog adoption", "cat adoption", "puppy adoption"

3. **AUDIENCE-INTENT CAMPAIGNS** (High intent)
   - Based on what people are searching for when they need you
   - Problem-aware searches
   - Example: "found stray dog what to do", "how to surrender a pet", "low cost spay neuter near me"

4. **LOCATION CAMPAIGNS** (If geographically focused)
   - City/region + service combinations
   - Example: "Kalispell pet adoption", "Flathead County animal shelter"

5. **CAUSE/AWARENESS CAMPAIGNS** (For education/advocacy orgs)
   - Issue-based searches
   - Example: "homeless youth statistics", "foster care crisis"

### Keyword Strategy by Match Type:

**EXACT MATCH [ ]** - Use for:
- High-intent, high-volume terms
- Brand terms
- Terms where meaning is unambiguous
- Example: [adopt a dog]

**PHRASE MATCH " "** - Use for:
- Medium-intent searches with modifiers
- Location + service combinations
- Example: "animal shelter near me"

**BROAD MATCH** - Use sparingly, only with:
- Maximize Conversions bidding
- Strong negative keyword lists
- Terms where you want discovery

### Ad Group Structure:
- 5-15 keywords per ad group (tightly themed)
- Keywords should share the same intent
- One clear landing page per ad group
- 2-3 RSAs per ad group

### Quality Score Optimization:
- Include primary keyword in at least 3 headlines
- Match ad copy tone to search intent
- Ensure landing page contains keyword themes
- Fast, mobile-friendly landing pages
"""

# ═══════════════════════════════════════════════════════════════════════════════
# RSA COPYWRITING FRAMEWORK
# ═══════════════════════════════════════════════════════════════════════════════

RSA_FRAMEWORK = """
## RSA (RESPONSIVE SEARCH AD) COPYWRITING FRAMEWORK

### Headlines (30 characters max each, provide exactly 15):

**Structure your 15 headlines across these categories:**

1-3: **KEYWORD HEADLINES** (include primary keyword)
   - "Adopt a Dog in [City]"
   - "[Service] Near You"
   - "Find Your Perfect [Animal/Program]"

4-5: **BRAND HEADLINES**
   - "[Org Name]"
   - "[Org Name] - [Tagline snippet]"

6-7: **DIFFERENTIATOR HEADLINES**
   - "No-Kill Shelter Since [Year]"
   - "[Stat] Animals Saved Last Year"
   - "100% [Unique claim]"

8-9: **BENEFIT HEADLINES**
   - "Give a Pet a Second Chance"
   - "Change a Life Today"
   - "Free [Service/Consultation]"

10-11: **URGENCY/CTA HEADLINES**
   - "Meet Your New Best Friend"
   - "Start Your Journey Today"
   - "Limited Spots Available"

12-13: **TRUST HEADLINES**
   - "Trusted Since [Year]"
   - "[X]+ Families Served"
   - "4.9★ Google Reviews"

14-15: **QUESTION/CURIOSITY HEADLINES**
   - "Ready to Make a Difference?"
   - "Looking for [Solution]?"

### Descriptions (90 characters max each, provide exactly 4):

**Description 1: VALUE PROPOSITION**
Full sentence explaining what you do and who you serve. Include keyword naturally.

**Description 2: DIFFERENTIATOR + PROOF**
What makes you unique + a stat or credential that proves it.

**Description 3: BENEFITS + EMOTION**
The emotional outcome or transformation. Appeal to why they should care.

**Description 4: CTA + LOGISTICS**
Clear call to action + any helpful details (hours, location, "free consultation", etc.)

### RSA Best Practices:
- Vary headline lengths (some short ~15 chars, some full 30)
- Don't repeat the same words across headlines
- Each headline should make sense standalone
- Avoid excessive punctuation (! limits to 1 per ad)
- No ALL CAPS
- Include numbers/stats when possible
- Pin only when absolutely necessary (reduces optimization)
"""

# ═══════════════════════════════════════════════════════════════════════════════
# ORG-TYPE SPECIFIC STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════

ORG_TYPE_STRATEGIES = {
    "animal": """
## ANIMAL WELFARE CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Adoption campaigns (by species, breed, age)
2. Foster/volunteer recruitment
3. Low-cost services (spay/neuter, vaccines)
4. Surrender/rehoming support
5. Lost & found / stray services
6. Sponsor-an-animal programs

**Keyword Themes That Convert:**
- "[species] adoption [city]"
- "adopt a [dog/cat/pet] near me"
- "animal shelter [city]"
- "no-kill shelter [region]"
- "low cost spay neuter [city]"
- "pet surrender options"
- "found stray [dog/cat] what to do"
- "foster a [dog/cat/pet]"
- "volunteer at animal shelter"
- "sponsor a shelter animal"

**Ad Copy Angles:**
- Second chance / new beginning
- Save a life / be a hero
- Your new best friend is waiting
- Don't shop, adopt
- Give love, get love
- Every animal deserves a family

**Negative Keywords to Include:**
- pet store, breeder, puppy mill, buy, purchase, for sale
- stuffed, toy, costume
- wild, zoo, exotic (unless you serve these)
- free puppies, free kittens (attracts wrong intent)
""",

    "legal": """
## LEGAL AID CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Practice area campaigns (eviction defense, family law, immigration)
2. Eligibility/intake campaigns ("free legal help", "legal aid near me")
3. Know Your Rights campaigns (educational)
4. Emergency/urgent legal help
5. Specific population campaigns (veterans legal help, senior legal aid)

**Keyword Themes That Convert:**
- "free legal help [city]"
- "legal aid [practice area]"
- "[practice area] lawyer low income"
- "pro bono [practice area] attorney"
- "tenant rights [city]"
- "eviction help [city]"
- "immigration lawyer free consultation"
- "expungement lawyer [city]"
- "help with [legal issue]"

**Ad Copy Angles:**
- Free legal help for those who qualify
- Know your rights
- You don't have to face this alone
- Experienced attorneys, no cost to you
- Protect your family / home / future
- Confidential consultation

**Negative Keywords to Include:**
- jobs, careers, salary, degree
- how to become, law school
- attorney fees, lawyer cost (unless addressing it)
- [opposing practice areas you don't handle]
""",

    "housing": """
## HOMELESS & HOUSING SERVICES CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Emergency shelter/immediate help
2. Housing programs by type (transitional, permanent supportive)
3. Prevention (eviction, utility assistance)
4. Population-specific (veterans, families, youth)
5. Supportive services (case management, job training)

**Keyword Themes That Convert:**
- "homeless shelter [city]"
- "emergency housing [city]"
- "help with rent [city]"
- "eviction prevention [city]"
- "transitional housing [city]"
- "housing assistance near me"
- "homeless youth shelter"
- "veteran housing help"
- "family shelter [city]"

**Ad Copy Angles:**
- Safe shelter tonight
- You are not alone
- Housing is a right
- First step to stability
- Case managers ready to help
- No judgment, just help

**Negative Keywords to Include:**
- for sale, rent apartment, listings, zillow
- luxury, affordable housing (unless you provide)
- section 8 waiting list (unless relevant)
- homeless statistics, research (unless educational)
""",

    "education": """
## EDUCATION NONPROFIT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Program enrollment campaigns (tutoring, after-school, summer)
2. Scholarship/financial aid campaigns
3. Parent engagement campaigns
4. Volunteer/mentor recruitment
5. College prep / access programs

**Keyword Themes That Convert:**
- "free tutoring [city]"
- "after school programs [city]"
- "summer camp [city] low cost"
- "scholarship for [demographic]"
- "college prep programs"
- "GED classes [city]"
- "mentoring programs for youth"
- "STEM programs for kids"

**Ad Copy Angles:**
- Every child deserves to succeed
- Unlock your potential
- Free for families who qualify
- Small class sizes, big results
- Build skills for the future
- Caring mentors, real results

**Negative Keywords to Include:**
- online degree, university, college (unless relevant)
- teacher jobs, education jobs
- curriculum for sale
- homeschool curriculum (unless you provide)
""",

    "health": """
## HEALTH & MEDICAL NONPROFIT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Service-specific campaigns (dental, vision, primary care)
2. Condition-specific campaigns (diabetes, cancer support)
3. Population-specific (uninsured, seniors, children)
4. Screening/prevention campaigns
5. Enrollment/eligibility campaigns

**Keyword Themes That Convert:**
- "free clinic [city]"
- "low cost [service] [city]"
- "sliding scale [service]"
- "community health center [city]"
- "free health screenings"
- "uninsured healthcare options"
- "[condition] support group"
- "free dental care [city]"

**Ad Copy Angles:**
- Healthcare for all, regardless of ability to pay
- Sliding scale fees based on income
- No insurance? No problem.
- Compassionate care you can afford
- Your health shouldn't wait
- Serving the community since [year]

**Negative Keywords to Include:**
- insurance quotes, health insurance plans
- medical school, nursing jobs
- [services you don't provide]
- luxury, cosmetic (unless relevant)
""",

    "food": """
## FOOD SECURITY CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Food distribution campaigns (pantry, meals)
2. Eligibility/enrollment campaigns (SNAP, WIC)
3. Volunteer/donation recruitment
4. Specific population campaigns (seniors, families)
5. Nutrition education programs

**Keyword Themes That Convert:**
- "food pantry [city]"
- "free food near me"
- "food bank [city]"
- "SNAP enrollment help"
- "meals on wheels [city]"
- "emergency food assistance"
- "food for families in need"
- "senior meal programs"

**Ad Copy Angles:**
- No one should go hungry
- Food for families in need
- No questions asked
- Dignity and respect for all
- Fresh, nutritious food
- Open to all who need help

**Negative Keywords to Include:**
- restaurant, delivery, grubhub, doordash
- recipes, cooking classes (unless you offer)
- food stamps application (unless you help with this)
- wholesale, bulk food purchase
""",

    "mental": """
## MENTAL HEALTH CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Service-type campaigns (counseling, crisis, support groups)
2. Condition-specific campaigns (anxiety, depression, trauma)
3. Population-specific (youth, veterans, LGBTQ+)
4. Crisis/urgent help campaigns
5. Education/awareness campaigns

**Keyword Themes That Convert:**
- "free counseling [city]"
- "low cost therapy [city]"
- "mental health help near me"
- "depression support group"
- "anxiety counseling"
- "crisis hotline"
- "teen mental health services"
- "trauma therapy [city]"
- "grief counseling"

**Ad Copy Angles:**
- You don't have to face this alone
- Healing is possible
- Confidential, compassionate care
- Sliding scale fees available
- First step toward feeling better
- Experienced, licensed therapists

**Negative Keywords to Include:**
- psychology degree, therapist jobs
- mental health statistics
- medication, prescription (unless relevant)
- online therapy apps (unless you are one)
""",

    "youth": """
## YOUTH & CHILDREN CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Program enrollment (after-school, summer, sports)
2. Parent/family campaigns
3. Teen-specific campaigns (jobs, mental health)
4. Mentoring/volunteer recruitment
5. Crisis services (runaway, homeless youth)

**Keyword Themes That Convert:**
- "after school programs [city]"
- "summer camp [city]"
- "youth programs near me"
- "teen job training"
- "mentoring programs"
- "activities for kids [city]"
- "help for troubled teens"
- "runaway youth shelter"
- "foster youth programs"

**Ad Copy Angles:**
- Every kid deserves a chance
- Building tomorrow's leaders
- Safe space to learn and grow
- Caring mentors, real connections
- Free for families who qualify
- Where kids become confident

**Negative Keywords to Include:**
- babysitting, daycare costs
- youth sports leagues (unless you run them)
- parenting advice, books
- troubled teen camps (negative connotation)
""",

    "veterans": """
## VETERANS SERVICES CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Benefits assistance campaigns
2. Service-specific (housing, employment, mental health)
3. Population-specific (combat veterans, women veterans)
4. Crisis/urgent help campaigns
5. Transition assistance

**Keyword Themes That Convert:**
- "veteran benefits help [city]"
- "VA claim assistance"
- "veteran housing [city]"
- "veteran job training"
- "PTSD treatment veterans"
- "veteran service organizations"
- "help for homeless veterans"
- "veteran mental health"
- "military to civilian transition"

**Ad Copy Angles:**
- You served us. Let us serve you.
- Benefits you've earned
- No veteran left behind
- Free help with VA claims
- Fellow veterans helping veterans
- Confidential support

**Negative Keywords to Include:**
- veteran jobs hiring
- military discount
- VA hospital reviews
- veteran owned business (unless relevant)
""",

    "environmental": """
## ENVIRONMENTAL NONPROFIT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Volunteer/stewardship campaigns
2. Membership/donation campaigns
3. Education program campaigns
4. Advocacy/action campaigns
5. Specific issue campaigns (clean water, trails, wildlife)

**Keyword Themes That Convert:**
- "volunteer [conservation activity] [city]"
- "protect [local landmark/species]"
- "environmental volunteer opportunities"
- "trail cleanup [city]"
- "tree planting volunteer"
- "wildlife conservation [region]"
- "clean water [city]"
- "environmental education programs"

**Ad Copy Angles:**
- Protect what you love
- Be part of the solution
- Leave it better than you found it
- Join [X] volunteers making a difference
- Local action, lasting impact
- For future generations

**Negative Keywords to Include:**
- environmental jobs, careers
- environmental science degree
- climate change debate
- pollution statistics (unless educational)
""",

    "faith": """
## FAITH-BASED NONPROFIT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Service-specific campaigns (food pantry, shelter, counseling)
2. Program campaigns (youth, recovery, support groups)
3. Community event campaigns
4. Volunteer/outreach campaigns

**Keyword Themes That Convert:**
- "[service] help [city]"
- "church food pantry [city]"
- "faith-based counseling"
- "christian [service] [city]"
- "[denomination] outreach"
- "community help [city]"
- "recovery program faith based"
- "support groups [city]"

**Ad Copy Angles:**
- Serving all in need, with love
- Faith in action
- Open to everyone, regardless of faith
- Compassionate help, no strings attached
- Community caring for community
- Hope and help for all

**Negative Keywords to Include:**
- church service times, mass times
- [other denominations if specific]
- religious debate, theology
- church jobs, pastor jobs
""",

    "substance": """
## SUBSTANCE RECOVERY CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Treatment type campaigns (detox, inpatient, outpatient)
2. Substance-specific campaigns (alcohol, opioid, meth)
3. Population-specific campaigns (women, veterans, youth)
4. Family/loved ones campaigns
5. Prevention/education campaigns

**Keyword Themes That Convert:**
- "drug rehab [city]"
- "alcohol treatment [city]"
- "free addiction help"
- "detox center [city]"
- "outpatient rehab"
- "opioid addiction treatment"
- "sober living [city]"
- "addiction counseling"
- "help for alcoholic family member"

**Ad Copy Angles:**
- Recovery is possible
- You are not alone
- Take the first step today
- Confidential assessment
- Insurance accepted / sliding scale
- Experienced, compassionate team

**Negative Keywords to Include:**
- addiction statistics
- celebrity rehab
- drug test, pass drug test
- [substances you don't treat]
""",

    "disability": """
## DISABILITY SERVICES CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Service-specific campaigns (employment, housing, therapy)
2. Disability-specific campaigns (autism, developmental, physical)
3. Age-specific campaigns (early intervention, transition, adult)
4. Family/caregiver campaigns
5. Advocacy/rights campaigns

**Keyword Themes That Convert:**
- "[disability] services [city]"
- "disability employment programs"
- "special needs programs [city]"
- "autism services [city]"
- "developmental disability services"
- "disability advocacy"
- "respite care [city]"
- "independent living services"
- "disability benefits help"

**Ad Copy Angles:**
- Empowering ability
- Independence with support
- Every person has potential
- Person-centered services
- Advocating for your rights
- Building skills for life

**Negative Keywords to Include:**
- disability lawyer (unless you provide)
- handicap parking
- disability check, SSDI amount
- special needs products for sale
""",

    "immigration": """
## IMMIGRATION & REFUGEE CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Legal service campaigns (asylum, citizenship, DACA)
2. Resettlement service campaigns
3. ESL/education campaigns
4. Employment/job training campaigns
5. Know Your Rights campaigns

**Keyword Themes That Convert:**
- "immigration lawyer free [city]"
- "citizenship help [city]"
- "DACA renewal help"
- "refugee services [city]"
- "ESL classes free [city]"
- "asylum application help"
- "immigration legal aid"
- "immigrant job training"
- "know your rights immigration"

**Ad Copy Angles:**
- You belong here
- Free legal help for immigrants
- Building a new home together
- Trusted guidance through the process
- Multilingual staff ready to help
- Defending immigrant rights

**Negative Keywords to Include:**
- immigration lawyer cost
- deportation news
- border patrol, ICE
- visa lottery, green card lottery (unless relevant)
""",

    "lgbtq": """
## LGBTQ+ SERVICES CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Service-specific campaigns (counseling, health, legal)
2. Youth-specific campaigns
3. Support group campaigns
4. Community event campaigns
5. Advocacy campaigns

**Keyword Themes That Convert:**
- "LGBTQ center [city]"
- "LGBT counseling [city]"
- "transgender health services"
- "LGBTQ youth support"
- "gay friendly therapist [city]"
- "coming out support"
- "LGBTQ support groups"
- "trans resources [city]"
- "pride events [city]"

**Ad Copy Angles:**
- You are not alone
- Safe, affirming space
- By the community, for the community
- Confidential support
- Celebrating who you are
- Help and hope for LGBTQ+ youth

**Negative Keywords to Include:**
- conversion therapy
- LGBT debate, controversy
- pride merchandise
- dating, apps
""",

    "arts": """
## ARTS & CULTURE CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Event/performance campaigns
2. Class/workshop enrollment campaigns
3. Membership campaigns
4. Youth program campaigns
5. Accessibility program campaigns

**Keyword Themes That Convert:**
- "[art form] classes [city]"
- "[art form] performances [city]"
- "theater tickets [city]"
- "art museum [city]"
- "kids art classes [city]"
- "free concerts [city]"
- "dance classes [city]"
- "art exhibits [city]"
- "community theater"

**Ad Copy Angles:**
- Experience the arts
- Discover your creative side
- Classes for all ages and abilities
- Affordable/free admission days
- Supporting local artists
- Art for everyone

**Negative Keywords to Include:**
- art for sale, buy art
- art degree, art school
- clip art, stock images
- karaoke, open mic (unless relevant)
""",

    "advocacy": """
## ADVOCACY & POLICY CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Issue awareness campaigns (educate about the issue)
2. Action/petition campaigns (get people to take action)
3. Event/rally campaigns
4. Resource/toolkit campaigns
5. Membership/supporter recruitment

**Keyword Themes That Convert:**
- "[issue] advocacy"
- "[issue] nonprofit"
- "how to help [issue]"
- "[issue] petition"
- "support [cause]"
- "[issue] facts"
- "[issue] action"
- "fight for [cause]"
- "[issue] organizations"
- "stand up for [cause]"

**Ad Copy Angles:**
- Your voice matters
- Join the movement
- Take action today
- Be part of the change
- [X] people have joined
- Together we can [outcome]

**Negative Keywords to Include:**
- [opposing viewpoint terms]
- news, breaking news
- statistics only (unless educational)
- debate, controversy
- jobs, careers in advocacy
""",

    "civic": """
## CIVIC ENGAGEMENT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Voter registration campaigns
2. Voter education campaigns
3. Civic education programs
4. Citizenship preparation
5. Community leadership programs

**Keyword Themes That Convert:**
- "register to vote [state]"
- "voter registration [city]"
- "how to vote [state]"
- "ballot information [city]"
- "citizenship classes [city]"
- "civic engagement programs"
- "community leadership training"
- "get involved in local government"
- "town hall meetings [city]"

**Ad Copy Angles:**
- Your vote is your voice
- Be an informed voter
- Make your voice heard
- Democracy starts with you
- Free, nonpartisan resources
- Empower your community

**Negative Keywords to Include:**
- [specific candidate names]
- [partisan terms if nonpartisan]
- voting machine problems
- election fraud
- political party registration (unless relevant)
""",

    "community": """
## COMMUNITY DEVELOPMENT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Homeownership/housing programs
2. Small business support campaigns
3. Financial empowerment programs
4. Workforce development
5. Neighborhood revitalization

**Keyword Themes That Convert:**
- "first time homebuyer programs [city]"
- "down payment assistance [city]"
- "small business loans [city]"
- "financial counseling [city]"
- "job training programs [city]"
- "credit counseling nonprofit"
- "community development [city]"
- "home repair assistance"
- "micro loans [city]"

**Ad Copy Angles:**
- Build wealth in your community
- Own your future
- Free financial coaching
- Invest in your neighborhood
- From renter to homeowner
- Support local businesses

**Negative Keywords to Include:**
- real estate listings
- mortgage rates, refinance
- payday loans, quick cash
- commercial real estate
- franchise opportunities
""",

    "disaster": """
## DISASTER RELIEF CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Emergency response campaigns (during/after disaster)
2. Volunteer recruitment campaigns
3. Donation campaigns
4. Preparedness education campaigns
5. Long-term recovery programs

**Keyword Themes That Convert:**
- "[disaster type] relief"
- "[disaster type] help [location]"
- "disaster volunteer opportunities"
- "donate to [disaster] victims"
- "disaster preparedness"
- "[disaster] recovery assistance"
- "emergency shelter [location]"
- "help [disaster] victims"
- "disaster relief organizations"

**Ad Copy Angles:**
- Help families rebuild
- Immediate relief, long-term recovery
- 100% of donations go to victims
- Volunteers needed now
- Be ready for emergencies
- Neighbors helping neighbors

**Negative Keywords to Include:**
- disaster movies, games
- insurance claims
- FEMA complaints
- disaster tourism
- news coverage only
""",

    "economic": """
## ECONOMIC DEVELOPMENT CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Small business development campaigns
2. Entrepreneurship programs
3. Workforce training campaigns
4. Microenterprise/microloan campaigns
5. Industry-specific programs

**Keyword Themes That Convert:**
- "small business help [city]"
- "start a business [city]"
- "business loans for [demographic]"
- "entrepreneur training"
- "free business classes [city]"
- "minority business development"
- "women business owners"
- "small business grants"
- "business incubator [city]"
- "SCORE mentor [city]"

**Ad Copy Angles:**
- Turn your idea into a business
- Free business coaching
- Loans for underserved entrepreneurs
- Build your dream
- Expert mentors, no cost
- From startup to success

**Negative Keywords to Include:**
- business for sale
- franchise cost
- MBA programs
- get rich quick
- dropshipping, passive income
""",

    "foundation": """
## FOUNDATION / GRANTMAKING CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Grant opportunity awareness campaigns
2. Scholarship campaigns
3. Nonprofit capacity building campaigns
4. Impact/annual report campaigns
5. Donor engagement campaigns

**Keyword Themes That Convert:**
- "grants for nonprofits [focus area]"
- "scholarship [type/demographic]"
- "foundation grants [city/state]"
- "[focus area] funding"
- "grant opportunities [sector]"
- "nonprofit capacity building"
- "community foundation [city]"
- "apply for grants"
- "[scholarship name]"

**Ad Copy Angles:**
- Funding that changes lives
- Apply for [amount] grants
- Scholarships available now
- Investing in [cause]
- Strengthening nonprofits
- Your community foundation

**Negative Keywords to Include:**
- grant writing jobs
- foundation jobs
- how to start a foundation
- government grants for individuals
- free money, personal grants
""",

    "historical": """
## HISTORICAL PRESERVATION CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Visitor/tourism campaigns
2. Membership campaigns
3. Event/tour campaigns
4. Education program campaigns
5. Preservation advocacy campaigns

**Keyword Themes That Convert:**
- "historic sites [city]"
- "history museum [city]"
- "historic house tour [city]"
- "[era] history [city]"
- "preservation society [city]"
- "historic walking tour"
- "local history [city]"
- "[cultural] heritage [city]"
- "historic landmark [city]"

**Ad Copy Angles:**
- Step back in time
- Discover local history
- Preserve the past for the future
- Tours available daily
- Member benefits
- Support historic preservation

**Negative Keywords to Include:**
- history channel, documentaries
- history degree, jobs
- antiques for sale
- old houses for sale
- historical fiction books
""",

    "human": """
## HUMAN SERVICES CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Emergency assistance campaigns
2. Service-specific campaigns (utility help, food, etc.)
3. Tax preparation (VITA) campaigns
4. Benefits enrollment campaigns
5. Holiday assistance campaigns

**Keyword Themes That Convert:**
- "help paying bills [city]"
- "emergency assistance [city]"
- "utility assistance programs"
- "rent help [city]"
- "free tax preparation [city]"
- "SNAP application help"
- "community assistance [city]"
- "help for families in need"
- "social services [city]"

**Ad Copy Angles:**
- Help when you need it most
- No judgment, just help
- Emergency assistance available
- Free, confidential services
- Serving neighbors in need
- One call connects you to help

**Negative Keywords to Include:**
- social services jobs
- government benefits fraud
- welfare statistics
- charity ratings
- social work degree
""",

    "international": """
## INTERNATIONAL AID CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Cause-specific campaigns (water, hunger, health)
2. Country/region campaigns
3. Child sponsorship campaigns
4. Emergency response campaigns
5. Volunteer/mission trip campaigns

**Keyword Themes That Convert:**
- "donate to [cause] [country/region]"
- "sponsor a child [country]"
- "clean water charity"
- "hunger relief [region]"
- "[country] humanitarian aid"
- "international volunteer"
- "mission trips [region]"
- "global poverty solutions"
- "overseas charity"

**Ad Copy Angles:**
- Change a life across the world
- $X provides [specific impact]
- 90%+ goes directly to programs
- Sponsor a child today
- Clean water saves lives
- Join our global community

**Negative Keywords to Include:**
- international news
- travel deals, flights
- foreign aid criticism
- charity scams
- volunteer abroad cost (unless relevant)
""",

    "literacy": """
## LIBRARY & LITERACY CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Program enrollment campaigns (tutoring, ESL, GED)
2. Children's literacy campaigns
3. Volunteer tutor recruitment
4. Book distribution campaigns
5. Digital literacy programs

**Keyword Themes That Convert:**
- "free tutoring [city]"
- "learn to read [city]"
- "adult literacy programs"
- "ESL classes free [city]"
- "GED classes [city]"
- "reading programs for kids"
- "volunteer tutor [city]"
- "children's literacy [city]"
- "free books for kids"

**Ad Copy Angles:**
- Reading opens doors
- It's never too late to learn
- Free tutoring for all ages
- Give the gift of literacy
- Volunteer tutors needed
- Every child deserves to read

**Negative Keywords to Include:**
- library jobs
- library science degree
- books for sale
- kindle, audiobooks
- speed reading courses
""",

    "racial": """
## RACIAL JUSTICE CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Education/awareness campaigns
2. Action/advocacy campaigns
3. Program enrollment campaigns
4. Community event campaigns
5. Resource/support campaigns

**Keyword Themes That Convert:**
- "racial justice organizations"
- "anti-racism training [city]"
- "racial equity programs"
- "[community] empowerment [city]"
- "diversity training nonprofit"
- "civil rights organizations [city]"
- "racial healing"
- "[community] community organizations"
- "social justice nonprofit"

**Ad Copy Angles:**
- Building an equitable future
- Education leads to action
- Join the movement
- Healing communities together
- Equity for all
- Your voice matters

**Negative Keywords to Include:**
- racial statistics (unless educational)
- controversial debates
- news stories only
- political party content
- hate group content
""",

    "science": """
## SCIENCE & RESEARCH CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Education program campaigns
2. Membership/visit campaigns (museums, centers)
3. Citizen science campaigns
4. Youth STEM campaigns
5. Research awareness campaigns

**Keyword Themes That Convert:**
- "science museum [city]"
- "STEM programs for kids [city]"
- "science camp [city]"
- "planetarium [city]"
- "nature center [city]"
- "citizen science projects"
- "science fair [city]"
- "astronomy events [city]"
- "science education nonprofit"

**Ad Copy Angles:**
- Spark curiosity
- Hands-on science for all ages
- Discover the wonders of science
- Future scientists start here
- Explore, learn, discover
- Science is for everyone

**Negative Keywords to Include:**
- science degree, jobs
- scientific journals, papers
- lab equipment for sale
- science news only
- science fiction
""",

    "sports": """
## SPORTS & RECREATION CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Registration/enrollment campaigns
2. Scholarship/fee assistance campaigns
3. Volunteer coach recruitment
4. Camp/clinic campaigns
5. Adaptive sports campaigns

**Keyword Themes That Convert:**
- "youth [sport] [city]"
- "kids [sport] league [city]"
- "[sport] camp [city]"
- "affordable sports programs"
- "sports for kids [city]"
- "volunteer coach [sport]"
- "adaptive sports [city]"
- "Special Olympics [city]"
- "after school sports"
- "summer sports camp"

**Ad Copy Angles:**
- Every kid deserves to play
- No child turned away for inability to pay
- Build skills, build character
- More than a game
- Coaches change lives
- Sports for all abilities

**Negative Keywords to Include:**
- professional [sport]
- [sport] tickets
- sports betting
- fantasy sports
- sports news, scores
- sports equipment sale
""",

    "technology": """
## TECHNOLOGY ACCESS CAMPAIGN STRATEGY

**High-Converting Campaign Types:**
1. Digital literacy class campaigns
2. Device distribution campaigns
3. Internet access campaigns
4. Coding/STEM programs
5. Senior technology programs

**Keyword Themes That Convert:**
- "free computer classes [city]"
- "digital literacy [city]"
- "learn computers seniors [city]"
- "free internet program"
- "coding classes for kids [city]"
- "computer donation [city]"
- "technology training nonprofit"
- "digital skills training"
- "affordable internet [city]"

**Ad Copy Angles:**
- Bridge the digital divide
- Technology skills for everyone
- Free computer classes
- Get connected
- No experience necessary
- Digital skills open doors

**Negative Keywords to Include:**
- computer repair
- buy computers, laptops for sale
- tech support
- IT jobs, certifications
- internet providers, plans
"""
}

# Default strategy for org types without specific guidance
DEFAULT_ORG_STRATEGY = """
## GENERAL NONPROFIT CAMPAIGN STRATEGY

**Campaign Structure:**
1. Brand campaign (org name variations)
2. Primary service campaigns (your main offerings)
3. Audience-intent campaigns (problem-aware searches)
4. Location campaigns (city/region + service)

**Keyword Research Approach:**
- Start with your services + location
- Think about what someone would search when they need you
- Include problem-aware searches ("help with X", "where to find X")
- Don't forget informational searches that lead to your services

**Ad Copy Focus:**
- Lead with the benefit to the searcher
- Include your differentiator
- Add credibility (years, people served, ratings)
- Clear call to action
"""

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSITE SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

class WebsiteScraper:
    """Intelligent website scraper for nonprofit sites."""
    
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')
        self.domain = urlparse(base_url).netloc
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
        self.visited = set()
        self.content = {}
        
    def scrape(self):
        """Scrape key pages from the website."""
        print(f"🌐 Scraping website: {self.base_url}")
        
        # Priority pages to find
        priority_paths = [
            '/', '/about', '/about-us', '/our-mission', '/mission',
            '/programs', '/services', '/what-we-do',
            '/donate', '/give', '/support', '/ways-to-give',
            '/volunteer', '/get-involved',
            '/contact', '/contact-us',
            '/adopt', '/adoption', '/foster',  # Animal-specific
            '/events', '/calendar',
        ]
        
        # Try priority pages first
        for path in priority_paths:
            if len(self.visited) >= MAX_PAGES_TO_SCRAPE:
                break
            url = urljoin(self.base_url, path)
            self._scrape_page(url)
        
        # If we haven't found enough, crawl from homepage
        if len(self.visited) < MAX_PAGES_TO_SCRAPE:
            self._crawl_links(self.base_url)
        
        return self._compile_content()
    
    def _scrape_page(self, url):
        """Scrape a single page."""
        if url in self.visited:
            return None
        if not url.startswith(('http://', 'https://')):
            return None
        if urlparse(url).netloc != self.domain:
            return None
            
        try:
            response = self.session.get(url, timeout=SCRAPE_TIMEOUT)
            response.raise_for_status()
            self.visited.add(url)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script, style, nav, footer
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                tag.decompose()
            
            # Get page title
            title = soup.title.string if soup.title else ''
            
            # Get meta description
            meta_desc = ''
            meta_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_tag:
                meta_desc = meta_tag.get('content', '')
            
            # Get main content
            main = soup.find('main') or soup.find('article') or soup.find('body')
            text = main.get_text(separator=' ', strip=True) if main else ''
            
            # Clean up text
            text = re.sub(r'\s+', ' ', text)
            text = text[:5000]  # Limit per page
            
            # Get headings for structure
            headings = [h.get_text(strip=True) for h in soup.find_all(['h1', 'h2', 'h3'])]
            
            # Determine page type
            path = urlparse(url).path.lower()
            page_type = self._classify_page(path, title.lower() if title else '')
            
            self.content[url] = {
                'url': url,
                'title': title,
                'meta_description': meta_desc,
                'headings': headings[:10],
                'text': text,
                'type': page_type
            }
            
            print(f"  ✓ Scraped: {path or '/'} ({page_type})")
            return soup
            
        except Exception as e:
            print(f"  ✗ Failed: {url} - {str(e)[:50]}")
            return None
    
    def _crawl_links(self, start_url):
        """Crawl links from a page."""
        soup = self._scrape_page(start_url)
        if not soup:
            return
            
        for link in soup.find_all('a', href=True):
            if len(self.visited) >= MAX_PAGES_TO_SCRAPE:
                break
            href = link['href']
            url = urljoin(start_url, href)
            if urlparse(url).netloc == self.domain:
                self._scrape_page(url)
    
    def _classify_page(self, path, title):
        """Classify page type."""
        if any(x in path or x in title for x in ['about', 'mission', 'story', 'history']):
            return 'about'
        if any(x in path or x in title for x in ['program', 'service', 'what-we-do']):
            return 'services'
        if any(x in path or x in title for x in ['donate', 'give', 'support', 'contribute']):
            return 'donate'
        if any(x in path or x in title for x in ['volunteer', 'involve', 'help']):
            return 'volunteer'
        if any(x in path or x in title for x in ['adopt', 'foster', 'rescue']):
            return 'adoption'
        if any(x in path or x in title for x in ['event', 'calendar']):
            return 'events'
        if any(x in path or x in title for x in ['contact']):
            return 'contact'
        if path in ['/', '']:
            return 'homepage'
        return 'other'
    
    def _compile_content(self):
        """Compile scraped content into structured summary."""
        if not self.content:
            return "No website content could be scraped."
        
        summary_parts = []
        
        # Group by page type
        by_type = {}
        for url, data in self.content.items():
            ptype = data['type']
            if ptype not in by_type:
                by_type[ptype] = []
            by_type[ptype].append(data)
        
        # Build summary
        for ptype in ['homepage', 'about', 'services', 'adoption', 'donate', 'volunteer', 'events', 'other']:
            if ptype not in by_type:
                continue
            for page in by_type[ptype]:
                summary_parts.append(f"""
### {page['title'] or page['url']} ({ptype.upper()})
URL: {page['url']}
Meta: {page['meta_description']}
Headings: {', '.join(page['headings'][:5])}
Content excerpt: {page['text'][:1500]}
""")
        
        return '\n'.join(summary_parts)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN CAMPAIGN GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

def generate_campaign_draft(intake_data: dict) -> dict:
    """
    Generate a complete Google Ads campaign draft from intake data.
    
    Args:
        intake_data: Dictionary containing all form submission data
        
    Returns:
        Dictionary containing complete campaign structure
    """
    
    # Initialize Anthropic client
    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Extract key fields
    org_name = intake_data.get('org_name', '')
    org_url = intake_data.get('org_url', '')
    org_type = intake_data.get('np_type', '')
    org_type_key = intake_data.get('np_type_key', '')
    city = intake_data.get('city', '')
    state = intake_data.get('state', '')
    mission = intake_data.get('mission', '')
    differentiator = intake_data.get('differentiator', '')
    impact_stats = intake_data.get('impact_stats', '')
    urgency_hook = intake_data.get('urgency_hook', '')
    
    # Parse org-specific fields
    org_fields = {}
    if intake_data.get('org_fields'):
        try:
            org_fields = json.loads(intake_data['org_fields'])
        except:
            pass
    
    # Parse conversions
    conversions = []
    if intake_data.get('conversions'):
        try:
            conversions = json.loads(intake_data['conversions'])
        except:
            pass
    
    # Get org-type specific strategy
    org_strategy = ORG_TYPE_STRATEGIES.get(org_type_key, DEFAULT_ORG_STRATEGY)
    
    # Scrape website for additional context
    website_content = ""
    if org_url:
        try:
            scraper = WebsiteScraper(org_url)
            website_content = scraper.scrape()
        except Exception as e:
            print(f"Website scraping failed: {e}")
            website_content = "Website could not be scraped."
    
    # Build the mega-prompt
    system_prompt = f"""You are an expert Google Ads strategist specializing in Google Ad Grants for nonprofits. You have 15+ years of experience managing millions in ad spend and have helped hundreds of nonprofits maximize their $10,000/month Ad Grants.

Your task is to create a complete, ready-to-implement Google Ads campaign structure for a nonprofit organization. Your output must be:
1. Fully compliant with Google Ad Grants policies
2. Optimized for high CTR (5%+ account average required)
3. Strategically structured for the specific nonprofit type
4. Written with compelling, conversion-focused ad copy

{AD_GRANTS_RULES}

{CAMPAIGN_STRATEGY}

{RSA_FRAMEWORK}

{org_strategy}
"""

    user_prompt = f"""# CAMPAIGN GENERATION REQUEST

## ORGANIZATION DETAILS

**Name:** {org_name}
**Type:** {org_type}
**Website:** {org_url}
**Location:** {city}, {state}

**Service Areas:** {intake_data.get('service_areas', 'Not specified')}

**Mission Statement:**
{mission}

**Unique Differentiator:**
{differentiator}

**Impact Statistics:**
{impact_stats}

**Urgency Hook:**
{urgency_hook}

**Elevator Pitch:**
{intake_data.get('elevator_pitch', 'Not specified')}

## ORG-SPECIFIC PROGRAM DETAILS

{json.dumps(org_fields, indent=2) if org_fields else 'No specific program details provided.'}

## CAMPAIGN GOALS

**Campaign Types Requested:** {intake_data.get('campaign_types', 'Not specified')}
**Primary Conversion Goal:** {intake_data.get('primary_goal', 'Not specified')}
**Secondary Goals:** {intake_data.get('secondary_goals', 'Not specified')}
**Success Definition:** {intake_data.get('success_definition', 'Not specified')}
**Seasonal Priorities:** {intake_data.get('seasonal', 'Not specified')}
**Upcoming Campaigns/Events:** {intake_data.get('upcoming_campaigns', 'Not specified')}

## TARGET AUDIENCE

**Audience Types:** {intake_data.get('audience_types', 'Not specified')}
**Ideal Supporter:** {intake_data.get('ideal_supporter', 'Not specified')}
**Age Ranges:** {intake_data.get('age_ranges', 'Not specified')}
**Geographic Targeting:** {intake_data.get('geo_targeting', 'Not specified')}
**Specific Areas:** {intake_data.get('specific_geo', 'Not specified')}

## MESSAGING PREFERENCES

**Tone:** {intake_data.get('tones', 'Not specified')}
**Tagline:** {intake_data.get('tagline', 'Not specified')}
**Keywords to INCLUDE:** {intake_data.get('include_keywords', 'Not specified')}
**Keywords to EXCLUDE:** {intake_data.get('exclude_keywords', 'Not specified')}
**Legal Restrictions:** {intake_data.get('legal_restrictions', 'Not specified')}
**Success Stories:** {intake_data.get('success_stories', 'Not specified')}
**Messaging to AVOID:** {intake_data.get('avoid_messaging', 'Not specified')}

### ⚠️ MUST INCLUDE IN ADS (CRITICAL)
The following are specific requirements from the client that MUST be incorporated into ad copy where relevant:

{intake_data.get('must_include', 'No specific requirements provided.')}

(Incorporate these elements naturally into headlines and descriptions. Do not ignore these requirements.)

## CONVERSION PAGES

{json.dumps(conversions, indent=2) if conversions else 'No conversion pages specified.'}

## WEBSITE CONTENT (Scraped)

{website_content}

---

# YOUR TASK

Generate a complete Google Ads campaign structure in the following JSON format. Be thorough, strategic, and creative.

```json
{{
  "meta": {{
    "org_name": "{org_name}",
    "org_type": "{org_type}",
    "generated_at": "ISO timestamp",
    "estimated_monthly_budget": "$10,000 (Ad Grants)",
    "recommended_bidding": "Maximize Conversions",
    "geographic_targeting": "description of targeting",
    "notes": "any strategic notes"
  }},
  "campaigns": [
    {{
      "name": "Campaign Name",
      "type": "brand|service|audience|location|awareness",
      "objective": "Why this campaign exists",
      "budget_allocation": "Percentage of monthly budget",
      "bidding_strategy": "Maximize Conversions or Target CPA",
      "geographic_targeting": "Location targeting details",
      "ad_groups": [
        {{
          "name": "Ad Group Name",
          "theme": "What this ad group targets",
          "landing_page": "Recommended landing page URL",
          "keywords": [
            {{"keyword": "[exact match keyword]", "match_type": "exact"}},
            {{"keyword": "\\"phrase match keyword\\"", "match_type": "phrase"}},
            {{"keyword": "broad match keyword", "match_type": "broad"}}
          ],
          "negative_keywords": ["negative1", "negative2"],
          "ads": [
            {{
              "headlines": [
                "Headline 1 (30 chars max)",
                "Headline 2",
                "...up to 15 headlines"
              ],
              "descriptions": [
                "Description 1 (90 chars max)",
                "Description 2",
                "Description 3",
                "Description 4"
              ],
              "final_url": "https://...",
              "path1": "path1",
              "path2": "path2"
            }}
          ]
        }}
      ]
    }}
  ],
  "account_level_negatives": [
    "List of negative keywords to apply at account level"
  ],
  "recommendations": {{
    "immediate_actions": ["Action 1", "Action 2"],
    "landing_page_suggestions": ["Suggestion 1"],
    "conversion_tracking_notes": ["Note 1"],
    "optimization_schedule": "When to review and optimize"
  }}
}}
```

IMPORTANT REQUIREMENTS:
1. Generate 3-6 campaigns based on the organization's needs
2. Each campaign must have 2-4 ad groups
3. Each ad group must have 10-20 keywords with appropriate match types
4. Each ad group needs 2 complete RSA ads with all 15 headlines and 4 descriptions
5. All headlines must be 30 characters or less
6. All descriptions must be 90 characters or less
7. Include the primary keyword in at least 3 headlines per ad
8. NO single-word keywords except brand name
9. Focus on high-intent, specific keywords that will maintain 5%+ CTR
10. Include robust negative keyword lists

OUTPUT ONLY VALID JSON. No markdown code blocks, no explanation before or after."""

    print("🤖 Calling Claude API for campaign generation...")
    
    # Call Claude API
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=16000,
        messages=[
            {"role": "user", "content": user_prompt}
        ],
        system=system_prompt
    )
    
    # Extract response
    response_text = response.content[0].text
    
    # Parse JSON from response
    try:
        # Try to find JSON in response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            campaign_data = json.loads(json_match.group())
        else:
            raise ValueError("No JSON found in response")
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print(f"Response preview: {response_text[:500]}")
        raise
    
    # Add intake metadata
    campaign_data['intake_data'] = {
        'org_name': org_name,
        'org_url': org_url,
        'org_type': org_type,
        'city': city,
        'state': state,
        'submitted_at': intake_data.get('submitted_at', datetime.now().isoformat())
    }
    
    return campaign_data


def save_draft(campaign_data: dict, slug: str) -> str:
    """Save campaign draft to file."""
    os.makedirs(DRAFTS_DIR, exist_ok=True)
    
    filepath = os.path.join(DRAFTS_DIR, f"{slug}.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(campaign_data, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Draft saved to: {filepath}")
    return filepath


def generate_slug(org_name: str) -> str:
    """Generate URL-safe slug from org name."""
    slug = org_name.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    slug = slug.strip('-')
    return slug[:50]


def send_slack_notification(campaign_data: dict, filepath: str):
    """Send Slack notification about generated draft."""
    if not SLACK_WEBHOOK:
        print("⚠️ No Slack webhook configured")
        return
    
    meta = campaign_data.get('meta', {})
    intake = campaign_data.get('intake_data', {})
    campaigns = campaign_data.get('campaigns', [])
    
    # Count totals
    total_ad_groups = sum(len(c.get('ad_groups', [])) for c in campaigns)
    total_keywords = sum(
        len(ag.get('keywords', []))
        for c in campaigns
        for ag in c.get('ad_groups', [])
    )
    
    # Build campaign summary
    campaign_summary = "\n".join([
        f"• *{c['name']}* ({c.get('type', 'unknown')}) - {len(c.get('ad_groups', []))} ad groups"
        for c in campaigns
    ])
    
    message = {
        "text": f"🎯 New Campaign Draft Generated: {intake.get('org_name', 'Unknown')}",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🎯 New Campaign Draft Generated"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Organization*\n{intake.get('org_name', 'Unknown')}"},
                    {"type": "mrkdwn", "text": f"*Type*\n{intake.get('org_type', 'Unknown')}"},
                    {"type": "mrkdwn", "text": f"*Location*\n{intake.get('city', '')}, {intake.get('state', '')}"},
                    {"type": "mrkdwn", "text": f"*Website*\n{intake.get('org_url', 'N/A')}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 Draft Summary*\n• {len(campaigns)} campaigns\n• {total_ad_groups} ad groups\n• {total_keywords} keywords"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🗂️ Campaigns*\n{campaign_summary}"
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Draft Location*\n`{filepath}`"
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Review and approve to push to Google Ads"}
                ]
            }
        ]
    }
    
    try:
        response = requests.post(SLACK_WEBHOOK, json=message, timeout=10)
        response.raise_for_status()
        print("📬 Slack notification sent")
    except Exception as e:
        print(f"⚠️ Slack notification failed: {e}")


def validate_draft(campaign_data: dict) -> list:
    """Validate campaign draft against Ad Grants rules."""
    issues = []
    
    campaigns = campaign_data.get('campaigns', [])
    
    for i, campaign in enumerate(campaigns):
        campaign_name = campaign.get('name', f'Campaign {i+1}')
        ad_groups = campaign.get('ad_groups', [])
        
        # Check minimum ad groups
        if len(ad_groups) < 2:
            issues.append(f"❌ {campaign_name}: Only {len(ad_groups)} ad group(s), minimum 2 required")
        
        for j, ag in enumerate(ad_groups):
            ag_name = ag.get('name', f'Ad Group {j+1}')
            keywords = ag.get('keywords', [])
            ads = ag.get('ads', [])
            
            # Check minimum ads
            if len(ads) < 2:
                issues.append(f"❌ {campaign_name} > {ag_name}: Only {len(ads)} ad(s), minimum 2 required")
            
            # Check keywords
            for kw in keywords:
                keyword = kw.get('keyword', '').strip('"[]')
                words = keyword.split()
                if len(words) == 1 and kw.get('match_type') != 'exact':
                    issues.append(f"⚠️ {campaign_name} > {ag_name}: Single-word keyword '{keyword}'")
            
            # Check ad copy lengths
            for k, ad in enumerate(ads):
                for h, headline in enumerate(ad.get('headlines', [])):
                    if len(headline) > 30:
                        issues.append(f"❌ {campaign_name} > {ag_name} > Ad {k+1}: Headline {h+1} too long ({len(headline)} chars): '{headline[:40]}...'")
                
                for d, desc in enumerate(ad.get('descriptions', [])):
                    if len(desc) > 90:
                        issues.append(f"❌ {campaign_name} > {ag_name} > Ad {k+1}: Description {d+1} too long ({len(desc)} chars)")
    
    if not issues:
        issues.append("✅ All validations passed!")
    
    return issues


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main entry point."""
    
    # Check for intake data (from file or environment)
    intake_file = os.environ.get('INTAKE_FILE', 'intake_data.json')
    
    if os.path.exists(intake_file):
        with open(intake_file, 'r', encoding='utf-8') as f:
            intake_data = json.load(f)
    else:
        # For testing, use sample data
        print("⚠️ No intake file found, using sample data for testing")
        intake_data = {
            "org_name": "Test Animal Shelter",
            "org_url": "https://example.org",
            "np_type": "Animal Welfare",
            "np_type_key": "animal",
            "city": "Sacramento",
            "state": "California",
            "mission": "To rescue, rehabilitate, and rehome abandoned animals in our community.",
            "differentiator": "Only no-kill shelter in the county",
            "impact_stats": "1,200+ animals saved last year, 95% adoption rate",
            "primary_goal": "Donate / Give Now",
            "campaign_types": "Donations, Adopt a pet, Volunteer recruitment"
        }
    
    print("=" * 60)
    print("🚀 SAP CAMPAIGN GENERATION ENGINE")
    print("=" * 60)
    print(f"Organization: {intake_data.get('org_name', 'Unknown')}")
    print(f"Type: {intake_data.get('np_type', 'Unknown')}")
    print("=" * 60)
    
    # Generate campaign draft
    campaign_data = generate_campaign_draft(intake_data)
    
    # Generate slug and save
    slug = generate_slug(intake_data.get('org_name', 'unknown'))
    filepath = save_draft(campaign_data, slug)
    
    # Validate
    print("\n📋 Validating draft...")
    issues = validate_draft(campaign_data)
    for issue in issues:
        print(f"  {issue}")
    
    # Send Slack notification
    send_slack_notification(campaign_data, filepath)
    
    print("\n✅ Campaign generation complete!")
    print(f"📁 Draft saved to: {filepath}")
    
    return campaign_data


if __name__ == "__main__":
    main()
