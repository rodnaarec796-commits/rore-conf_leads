"""Дашборд «Конференция 2026»: amoCRM -> data.json.
Переменные окружения: AMO_DOMAIN (roregroup.amocrm.ru), AMO_TOKEN, NEWBIZ_PIPELINE_ID.
Необязательные: WINDOW_DAYS (30), TOLERANCE_HOURS (24)."""
import os, json, time, requests
from datetime import datetime, timezone

DOMAIN = os.environ.get("AMO_DOMAIN", "roregroup.amocrm.ru")
HDR = {"Authorization": "Bearer " + os.environ["AMO_TOKEN"]}
CONF_PIPE = 10631686
NEWBIZ_PIPE = int(os.environ["NEWBIZ_PIPELINE_ID"])
F_NEWLEAD, F_REASON, F_STAFF = 1205073, 1205019, 1204799
STAFF_REASON = "Сотрудник"
WINDOW = int(os.environ.get("WINDOW_DAYS", 30)) * 86400
TOL = int(os.environ.get("TOLERANCE_HOURS", 24)) * 3600

GROUPS = {
    "Не наша аудитория": ["Не целевой контакт", "Не профильная компания", "Не наш сегмент / рынок", "Нет потребности"],
    "Не время": ["Не актуально сейчас", "Нет бюджета", "Уже работает с другим подрядчиком"],
    "Нет контакта": ["Не отвечает / не выходит на связь", "Отказался от дальнейшего контакта", "Некорректные контактные данные"],
    "Мусор и свои": ["Спам / мусорный лид", "Дубль", "PR-партнер", "Конкурент"],
    "Уже в работе": ["Действующий клиент", "Действующий партнер", "Действующий лид"],
}
GROUP_OF = {r: g for g, rs in GROUPS.items() for r in rs}


def get(path, params=None):
    for _ in range(5):
        r = requests.get(f"https://{DOMAIN}{path}", headers=HDR, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(2); continue
        if r.status_code == 204:
            return {}
        r.raise_for_status()
        time.sleep(0.15)
        return r.json()
    raise RuntimeError("amoCRM rate limit")


def paged(path, key, params, limit=250):
    page = 1
    while True:
        d = get(path, {**params, "limit": limit, "page": page})
        items = d.get("_embedded", {}).get(key, [])
        if not items:
            return
        yield from items
        page += 1


def cf(lead, fid):
    for f in lead.get("custom_fields_values") or []:
        if f["field_id"] == fid and f.get("values"):
            return f["values"][0]
    return None


def leads_of(pipe):
    return list(paged("/api/v4/leads", "leads",
                      {"filter[pipeline_id]": pipe, "with": "contacts,loss_reason"}))


def stages_of(pipe):
    d = get(f"/api/v4/leads/pipelines/{pipe}")
    st = sorted(d["_embedded"]["statuses"], key=lambda s: s["sort"])
    return [s for s in st if s["id"] not in (142, 143)]


def ids(lead, key):
    return [x["id"] for x in lead.get("_embedded", {}).get(key, [])]


def main():
    conf, nb = leads_of(CONF_PIPE), leads_of(NEWBIZ_PIPE)

    # --- база: все лиды -> минус сотрудники -> чистые
    clean, by_toggle, by_reason = [], 0, 0
    for l in conf:
        staff_t = bool((cf(l, F_STAFF) or {}).get("value"))
        rv = cf(l, F_REASON)
        staff_r = bool(rv and rv.get("value") == STAFF_REASON)
        by_toggle += staff_t
        by_reason += staff_r
        if not (staff_t or staff_r):
            clean.append(l)
    both = sum(1 for l in conf if (cf(l, F_STAFF) or {}).get("value")
               and (cf(l, F_REASON) or {}).get("value") == STAFF_REASON)

    taken = [l for l in clean if (cf(l, F_NEWLEAD) or {}).get("value")]
    refused = [l for l in clean if cf(l, F_REASON) and l not in taken]
    both_flags = [l for l in clean if l in taken and cf(l, F_REASON)]
    reasons = {}
    for l in refused:
        v = cf(l, F_REASON)["value"]
        reasons[v] = reasons.get(v, 0) + 1

    # --- когда включили ползунок «Новый лид»
    toggle_ts = {}
    for e in paged("/api/v4/events", "events",
                   {"filter[type][]": f"custom_field_{F_NEWLEAD}_value_changed",
                    "filter[entity][]": "lead"}, limit=100):
        toggle_ts[e["entity_id"]] = max(toggle_ts.get(e["entity_id"], 0), e["created_at"])

    # --- связка: тот же контакт (приоритет) или компания
    idx_c, idx_co = {}, {}
    for d in nb:
        for c in ids(d, "contacts"): idx_c.setdefault(c, []).append(d)
        for c in ids(d, "companies"): idx_co.setdefault(c, []).append(d)
    matched, no_match, multi, by_c, by_co = {}, 0, 0, 0, 0
    unmatched = []
    for l in taken:
        t = toggle_ts.get(l["id"], l["updated_at"])
        ok = lambda d: t - TOL <= d["created_at"] <= t + WINDOW
        for src, index, key in (("c", idx_c, "contacts"), ("co", idx_co, "companies")):
            cands = {d["id"]: d for k in ids(l, key) for d in index.get(k, []) if ok(d)}
            if cands:
                multi += len(cands) > 1
                d = min(cands.values(), key=lambda x: x["created_at"])
                matched[l["id"]] = d
                by_c += src == "c"; by_co += src == "co"
                break
        else:
            no_match += 1
            cs, cos = ids(l, "contacts"), ids(l, "companies")
            found = any(idx_c.get(k) for k in cs) or any(idx_co.get(k) for k in cos)
            unmatched.append({
                "id": l["id"], "name": l["name"],
                "toggled": datetime.fromtimestamp(t, timezone.utc).strftime("%d.%m.%Y"),
                "why": ("сделки NEWBIZ есть, но вне окна дат" if found else
                        "в NEWBIZ нет сделок с этим контактом или компанией" if (cs or cos) else
                        "к лиду не привязаны контакт и компания")})

    # --- этапы NEWBIZ по истории смен статусов
    st = stages_of(NEWBIZ_PIPE)
    pos = {s["id"]: i for i, s in enumerate(st)}
    reached = {d["id"]: pos.get(d["status_id"], len(st) - 1 if d["status_id"] == 142 else 0) for d in nb}
    for e in paged("/api/v4/events", "events",
                   {"filter[type][]": "lead_status_changed", "filter[entity][]": "lead"}, limit=100):
        after = (e.get("value_after") or [{}])[0].get("lead_status", {})
        if e["entity_id"] in reached and after.get("pipeline_id") == NEWBIZ_PIPE and after.get("id") in pos:
            reached[e["entity_id"]] = max(reached[e["entity_id"]], pos[after["id"]])

    def funnel(deals, base):
        out = []
        for i, s in enumerate(st):
            n = sum(1 for d in deals if reached[d["id"]] >= i)
            prev = out[-1]["n"] if out else len(deals)
            out.append({"name": s["name"], "n": n,
                        "step": round(n / prev * 100, 1) if prev else 0,
                        "cum": round(n / len(deals) * 100, 1) if deals else 0,
                        "of_clean": round(n / base * 100, 2) if base else 0})
        return out

    def outcome(deals):
        lost_r = {}
        for d in deals:
            if d["status_id"] == 143:
                lr = (d.get("_embedded", {}).get("loss_reason") or [{"name": "Не указана"}])[0]["name"]
                lost_r[lr] = lost_r.get(lr, 0) + 1
        return {"won": sum(d["status_id"] == 142 for d in deals),
                "lost": sum(d["status_id"] == 143 for d in deals),
                "open": sum(d["status_id"] not in (142, 143) for d in deals),
                "lost_reasons": lost_r}

    mine = list({d["id"]: d for d in matched.values()}.values())
    mine_ids = {d["id"] for d in mine}
    rest = [d for d in nb if d["id"] not in mine_ids]

    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="minutes"),
        "base": {"all": len(conf), "staff_toggle": by_toggle, "staff_reason": by_reason,
                 "staff_both": both, "clean": len(clean)},
        "split": {"taken": len(taken), "refused": len(refused),
                  "undecided": len(clean) - len(taken) - len(refused),
                  "matched": len(matched)},
        "reasons": [{"name": k, "n": v, "group": GROUP_OF.get(k, "Прочее")}
                    for k, v in sorted(reasons.items(), key=lambda x: -x[1])],
        "newbiz": {"deals": len(mine), "funnel": funnel(mine, len(clean)), **outcome(mine),
                   "baseline": {"deals": len(rest), "funnel": funnel(rest, 0), **outcome(rest)}},
        "quality": {"taken_no_match": no_match, "multi_candidates": multi,
                    "matched_by_contact": by_c, "matched_by_company": by_co,
                    "taken_and_refused": len(both_flags), "unmatched": unmatched},
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
