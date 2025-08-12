# ET Heatmap MVP (Hardened v1.1)

## Objective (non-negotiable)
Live, self-updating heatmap **plus actionable alerts** that beat trades by **≥ 2–6 hours** with **≥ 80%** editorial usefulness.

---

## Inputs (day-1)
- **Reddit API** (r/all + targeted subs)
- **Google Trends** (Trending Now + Explore; US + Entertainment)
- **TikTok Creative Center** (songs/hashtags/creators) via **Apify** actor
- **1–2 trade RSS** (Variety / THR) for confirmation only

**References**
- Google for Developers — Google Trends  
- TikTok For Business — Creative Center  
- Apify

---

## Ingestion & Resilience
- **Rate-aware scheduler** + **Redis cache** (TTLs as defined) + **exponential backoff**
- **Per-source health monitor** and **circuit breaker**
- **Normalization layer** to canonicalize entities; dedupe aliases
- **Audit logs** for every fetch, cache hit, and 429

---

## Entity Discovery (auto-add)
Add an entity when:
- Seen in **≥ 2 source-types** inside **12h**, **and**
- Initial **velocity z ≥ 1.8**  
Store **first-seen** timestamp and **provenance**.

---

## Scoring (rules, explainable)
HEAT = 0.5Velocity_z + 0.3Platform_Spread + 0.2*Affect


- **Velocity_z:** 12h vs. trailing 7d (capped at **4.0**)
- **Platform_Spread:** unique source-types / **3** (Reddit, Trends, TikTok)
- **Affect:** |sentiment| in **[-1, 1]**, mapped to **[0, 1]**; controversy bonus only if **volume > floor**
- **Freshness decay:** `HEAT * exp(-hours_since_peak / 24)`
- **Gates to alert eligibility:**  
  - **Velocity ≥ 2.5**  
  - **Spread ≥ 0.67** (≥ 2 types)  
  - **Persistence:** passes thresholds on **2× polls**

---

## Outputs

### Dashboard
- **Top 20** by HEAT
- Toggles for **velocity** / **spread**
- **Source receipts** and **source health**

### Slack Alerts (actionable)
- **Title:** `[ALERT] {ENTITY} spiking now`
- **Why:** `Velocity +{x}σ | Spread {2/3} | Affect {0.7} | Confidence {0.82}`
- **Buttons:** **Assign Producer**, **Create Rundown Card**, **Promo Tease**, **Open Research Pack** (webhook + modal)

**References**
- Slack API

### “Action Package” (auto-generated)
- **Booking brief:** 3 angles, likely hook, related names
- **Promo:** 3× on-air tease lines, 1× push alert, 1× social tease
- **Graphics/B-roll:** lower-third text, asset cues
- **Receipts:** top 3 links (Reddit / TikTok / Trends) with timestamps

---

## KPIs (tracked daily)
- **Lead-time vs. trades:** median **≥ 2–6h**
- **Alert usefulness:** **≥ 80%** “useful” votes
- **False-positive rate:** **≤ 15%** (items failing persistence/spread on backcheck)

---

## Tech Stack
- **FastAPI** + **Postgres** (entities/scores/history) + **Redis** (cache/rate)
- **Workers:** async fetchers per source; token-bucket + backoff; health metrics
- **Slack:** Incoming Webhook + Block Kit actions for assignments and modals

**References**
- Slack API

---

## Guardrails & Compliance
- Respect **Reddit OAuth** limits and platform **ToS**; log 429s and **auto-throttle**
- Use **cached Creative Center** data when DOM shifts; **do not brute-force**
