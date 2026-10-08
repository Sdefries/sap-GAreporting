# Reports site: reports.sponsorapurpose.org

Each client signs in to their own report with their email: they type it, get a one-time code, and stay signed in for 30 days. No passwords to set, share or reset. The login is Cloudflare Access (free for up to 50 people).

| Address | Who can open it |
|---|---|
| `reports.sponsorapurpose.org/<client>/` | That client's people, plus the agency |
| `reports.sponsorapurpose.org/<client>/request/` | Same: their campaign request portal |
| `reports.sponsorapurpose.org/demo/` | Anyone (for prospects) |
| `reports.sponsorapurpose.org/admin/` and `/` | The agency only (the admin page also needs the admin key) |

Any page without its own rule falls under the agency-only rule, so nothing is public by accident.

## Who can open a report

On the admin page, under each client, list who can open it, separated by commas:

- `director@pinkpaws.org`: one person
- `@pinkpaws.org`: everyone with an email at their organization

Free-mail domains such as `@gmail.com` are refused, because that would let in anyone with a Gmail account. Add Gmail users one by one. Save, then **Rebuild reports now**: the logins update within a few minutes. Removing someone takes effect at the next rebuild. Anyone already signed in is let in until their 30-day session ends; to cut someone off at once, revoke their session in Cloudflare Zero Trust → My Team → Users.

## One-time setup

1. **Make the GitHub repo private** (Settings → General → Danger zone → Change visibility). It's public now, so reports and `clients.json` can be read on GitHub whatever the website login does. Then turn off GitHub Pages (Settings → Pages).
2. **Cloudflare Zero Trust**: in the Cloudflare dashboard open Zero Trust and pick the Free plan if asked. Under Settings → Custom pages you can add the Sponsor a Purpose logo to the login page.
3. **Pages project**: Workers & Pages → Create → Pages → *Direct upload*, named `sap-reports`. Upload any small folder; the workflow replaces it. Then Custom domains → add `reports.sponsorapurpose.org`. Cloudflare creates the DNS record for you.
4. **API token**: My Profile → API Tokens → Create custom token with these permissions:
   - Account → Cloudflare Pages → Edit
   - Account → Access: Apps and Policies → Edit
   - Account → Access: Organizations, Identity Providers, and Groups → Edit
5. **GitHub secrets and variables** (repo → Settings → Secrets and variables → Actions):

   | Name | Kind | Value |
   |---|---|---|
   | `CLOUDFLARE_API_TOKEN` | Secret | The token from step 4 |
   | `CLOUDFLARE_ACCOUNT_ID` | Secret | On the right of the Cloudflare dashboard home |
   | `AGENCY_EMAILS` | Variable | `@sponsorapurpose.org,@sponsorapet.org`: who can open everything |
   | `REPORTS_DOMAIN` | Variable | Optional; defaults to `reports.sponsorapurpose.org` |
   | `PAGES_PROJECT` | Variable | Optional; defaults to `sap-reports` |

6. **Worker**: redeploy it (`cd worker && npx wrangler deploy`) so the reports site can use the live checks (`ALLOWED_ORIGINS` now includes `https://reports.sponsorapurpose.org`).
7. Run **Actions → SAP Ad Grants Automation → Run workflow → reports**. The "Publish to reports site" step:
   1. turns on email-code login;
   2. creates the logins;
   3. publishes the site.

   Open a client's address in a private window to check that it asks for an email.

Until `CLOUDFLARE_API_TOKEN` is set, the publish step is skipped and nothing changes.

## How it works

`python site.py access` creates one Cloudflare Access app per client from `report_access` in `clients.json` (add `--dry-run` to print the rules without calling Cloudflare). It also creates:

- a site-wide agency-only app;
- public apps for `/demo` and `/assets` (logos);
- agency-only apps for `sap-reports.pages.dev`, so the Pages address can't be used to get around the login.

It only touches apps whose names start with `sap-reports:`, and it never deletes before creating, so no page is left open while the logins change.

`python site.py build` copies `reports/`, `portals/` and `assets/` into `site/` in the folder layout above. It also adds headers that keep the pages out of search engines and out of shared caches.
