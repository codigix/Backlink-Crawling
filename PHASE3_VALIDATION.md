# Phase 3 classification accuracy audit

## A. Genuine editorial evidence for Codigix

**No independent editorial backlink to Codigix was verified.** Free searches covered the domain, company name, agency comparisons, founder interviews and industry mentions. I also checked local fixtures and crawler storage, and streamed the public backlink and outgoing-link tables from the three November 12 PostgreSQL backups without restoring them or connecting to the production tunnel. Across those tables I found 91 distinct Moraya source URLs pointing to Codigix. Many pages carrying the same credit are evidence of one template, not 91 editorial endorsements.

The following classifications used the downloaded source HTML, the crawler's own page extractor, and an empty relationship configuration.

| Actual page | Link and context | Expected / observed |
|---|---|---|
| [Moraya Hospital treatments](https://morayamultispecialitycharitablehospital.com/treatments.html) | Footer links "Codigix infotech Pvt Ltd" to the Codigix homepage immediately after "Website Designed by". | client_credit / client_credit, 0.88 |
| [Kitchen Canvas homepage](https://kitchen-canvas.in/) | Two footer placements link "Codigix Infotech" to the homepage after "Design By". | client_credit / client_credit for both, 0.88 |
| [beBee company page](https://bebee.com/in/companies/codigix-infotech-software-development-digital-marketing-agency) | The company profile links the bare address to https://codigix.co. There is no surrounding editorial sentence. | profile / profile, 0.95 |

The search also surfaced [a Peerlist profile](https://peerlist.io/company/codigix), [the company's LinkedIn page](https://in.linkedin.com/company/codigix-infotech), and [an aggregation of Tumblr posts](https://www.tumgik.com/tag/MedicalWebsiteDesigning). Direct requests returned HTTP 403. Other directory leads, including AeroLeads, ZaubaCorp, The Company Check and Planetexim, also returned 403. I did not classify search snippets or infer independence from publication on a different host. The independent-article tests use synthetic HTML. No live-positive Codigix test was manufactured.

## B. Previously named sources and targets

| Investigation | Evidence and outcome |
|---|---|
| GrowthPixelAgency | The source named by the fixture, [Daask Design](https://daaskdesign.com/), returned a complete 4,677,041-byte page. Both homepage links to https://growthpixelagency.com/ have anchor "GrowthPixel Pvt Ltd" immediately after development-credit wording in the footer. Expected and observed **client_credit**, 0.88. The older synthetic footer omits the wording and correctly yields sitewide for that different input. |
| BrainMineTech | [Brainmine's source homepage](https://brainmine.co.in/) yielded 42 HTML links to brainminetech.com. All 42 classified **sitewide**, including "Blog" in the header (0.90) and "Home" in navigation (0.92). The source uses the same brand. These placements provide no evidence of independent editorial coverage or configured ownership. |
| Ruby Hospital | [The WordPress bone-marrow article](https://generalrubyhospital.wordpress.com/2026/07/30/which-cancer-centre-in-kolkata-offers-bone-marrow-transplant-and-haemato-oncology-treatment/) contained seven target links. The article describes Ruby's programme and links phrases including "haematology" to its service pages. All seven were **unknown**, 0.40. The publisher account resembles the hospital's name and ownership was unconfirmed. The specific emergency URL in the synthetic fixture did not occur. The fixture's [swheal URL](https://swheal.com/hospital/ruby) redirected to [the current Ruby listing](https://swheal.com/hospital/kolkata/ruby-general-hospital/). Its HTML contains an email and social profiles, with zero HTTP links to the hospital domain. The link's absence today does not establish historical removal. |
| Infobel | The backups contained the platform root. Neither the Pune category URL nor the profile-style URL from the fixtures supplied an authenticated original listing. Direct fetches returned 403. The exact historical source and its link remain unverified. [Tested category URL](https://www.infobel.com/en/india/it/pune) |
| Crunchbase | The backups contained the platform root. The organization URL from the test returned 403. Its company-profile HTML is a test fixture. Live link presence, removal and disappearance cannot be determined. [Tested organization URL](https://www.crunchbase.com/organization/codigix-infotech-private-limited) |

## C. False positives and false negatives

The original editorial score could borrow words from other sentences in the same block. Classification now requires words in the link's own sentence, and recognizes image and button controls separately. A long article, a publication date or BlogPosting schema cannot supply that missing evidence. Ambiguous short prose produces unknown. A footer credit phrase belonging to a different party no longer transfers to the target merely because the links share a sentence. Explicit profile schema alongside strong competing listing evidence can yield unknown.

A confirmed client's independent citation was a false negative under the previous blanket rule. Link context now determines whether that citation can qualify. Inline ambiguous credit language and descriptions of a client engagement remain conservative. Short but genuinely editorial citations may also remain unknown. This audit does not estimate population precision or recall because there is no representative labeled sample.

## D. Repetition and crawl order

Previously each page was published once, using only pages known at arrival. A retry of a changed page could also leave obsolete destination and context counts. The tracker now replaces the observations for a source URL. Identical retries contribute once. Shared blogging accounts have separate template counts.

PageClassifier publishes provisional results while collecting evidence. Its explicit finalize() method recomputes every observed page against the completed sample, replaces retained results and emits final replacements to the callback. Consumers key replacements by source URL. Finalization is idempotent, clears the retained replay inputs, and closes that classifier to new pages. The finite run_crawl entry point calls it after its last round. Callers managing their own rounds select their crawl boundary and call finalize(). Results describe the observed crawl. A small sample cannot establish how every page of a website behaves.

Tests permute four pages through all 24 orders, include identical retries, replace changed observations, exercise streaming-only output, and cover the reverse transition when later pages dilute a provisional template match.

## E. Ownership and client relationships

The JSON loader takes explicit owned_domains, owned_accounts and client_domains for each target. No production relationship file was supplied or added. Internal links and configured properties can be recognized as owned. A bare shared platform cannot confirm ownership of every account. A brand resemblance can lower certainty but cannot prove ownership. Supplying a client relationship strengthens a design credit and supports a header/footer logo credit. A plain navigation link on that client still classifies sitewide. The named live sites were all evaluated without inventing a client relationship.

## F. Confidence

These are deterministic rule scores. They have not been fitted as probabilities. The live beBee profile scored **0.95**; the live Moraya and Kitchen Canvas credits scored **0.88**; the unconfirmed Ruby account's links scored **0.40**. In a controlled paired comparison, the same contextual citation scored **0.90** with plain attributes, **0.90** with nofollow, and **0.80** with sponsored. Both qualifiers survived in the results. Adding conflicting blog, directory and organization metadata reduced the plain citation to **0.78**. A thin page with only a visit link produced **unknown, 0.25**.

## G. Tests

The supplied pre-edit Phase 3 tree was copied before editing and collected 649 tests. The full post-change comparison collected 658. The final focused suite collected 69 classifier tests (the original 58 plus 11) and **all 69 passed**, including PostgreSQL/local-HTTP and Playwright integration. The final two edge cases check short direct-parent credit wording and require the repeated context itself to meet the minimum page count. The full comparison and the final focused rerun are reported separately to avoid double-counting.

The supplied summary reported two failures without naming them. Our full baseline surfaced three. All three also failed independently on a clean export of Git commit 9b010ae:

- tests/test_competitors.py::test_competitors_crawl_to_completion_one_slot_at_a_time_across_timeout_and_crash observed two active competitor jobs with a cap of one.
- tests/test_phase12.py::test_target_backlinks_keyset_and_offset_cover_every_row_once reported 143 backlinks for a 252-row expected result.
- tests/test_scenarios.py::test_api_root_domain_subdomain_pagination_and_statuses returned five backlinks where the test expected ten for a root target including its subdomain.

All database tests used a disposable, loopback-only PostgreSQL 18 cluster. Sources and fixtures were isolated from the working tree. The pre-edit run finished with **646 passed, 3 failed** (905.48 s). The revised full run finished with **655 passed, the same 3 failed** (830.42 s). The final 69-test classifier pass took 35.44 s. No new failure appeared in the Phase 1 or Phase 2 suites.

## H. Performance

Milliseconds per page, measured on this Windows host with Python 3.12.10. Observe includes the provisional classification. Finalize is its incremental cost across the crawl.

| Outgoing links | Filler words | Monitored targets | HTML parse + extraction | Pre-edit observe | Revised, repetition off | Revised observe | Finalize |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 30 | 500 | 1 | 9.11 | 0.96 | 0.71 | 0.74 | 0.08 |
| 30 | 500 | 8 | 9.11 | 1.64 | 1.13 | 1.75 | 0.43 |
| 500 | 8,000 | 1 | 168.80 | 15.92 | 14.33 | 14.72 | 0.06 |
| 500 | 8,000 | 8 | 168.80 | 21.75 | 15.00 | 24.08 | 0.82 |

Each page contained one contextual link per monitored target. Unmonitored links filled the remaining slots. Classification reused extracted facts across 12 distinct source URLs to isolate classifier cost. Figures are medians of five timed crawls after a warm-up; extraction is a separate five-run median. With 500 links and eight targets, collecting and finalizing together cost 24.90 ms/page, against 21.75 ms/page for the original provisional-only output. Replay did no HTML parsing, database access or network work. Measurements overlap the regression runs and should be read as local timings.

## I. Files changed by this audit

- backlink_classifier.py implements per-link evidence rules, scoped repetition tracking, provisional output and finalization.
- crawler_engine.py finalizes an attached classifier at the finite crawl boundary.
- tests/test_backlink_classifier.py strengthens the original expectations and covers the new behavior.
- PHASE3_VALIDATION.md records the evidence and acceptance decision.

## J. Limitations

Live HTML can change. Blocked pages and the unverified Codigix positive remain unresolved. Single-page checks cannot measure repetition across an entire publisher. Finalization holds compact evidence for monitored links in memory until the crawl ends. There is no cross-process reconciliation for independent crawler instances. Unrelated publisher accounts, editorial independence and customer relationships cannot always be established from HTML alone.

## K. Decision

**Accept Phase 3 under the documented-blocker criterion.** The local classification, reconciliation, confidence and regression checks passed for the requested scope. Independent real-world Codigix editorial accuracy remains unproven. Infobel and Crunchbase remain live-validation gaps. The three independently reproduced baseline failures concern the existing scheduler and API behavior. Work stopped at Phase 3.
