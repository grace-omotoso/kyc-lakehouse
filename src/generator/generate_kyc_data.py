#!/usr/bin/env python3
"""
Synthetic KYC/AML data generator for the S3 -> Databricks lakehouse project.

Produces three feeds in a landing layout that mirrors S3:

  landing/customers/dt=YYYY-MM-DD/customers.csv        (day 1 = full load, later days = changes + new customers)
  landing/transactions/dt=YYYY-MM-DD/transactions_001.json   (JSON lines)
  landing/watchlist/watchlist.csv                      (static reference list)

Planted on purpose (so Silver/quality/SCD2 work has something to do):
  - duplicate txn_id rows, null txn_id, negative/zero amounts, unparseable timestamps
  - schema drift: a new column `channel` appears from day 3 onward
  - a late-arriving file for day 2 dropped in after day N has landed (--late)
  - customer attribute changes over time (address, risk_rating) for SCD Type 2
  - AML scenarios: structuring (many txns just under 10,000), high-risk-country
    activity, and customers whose names match the synthetic watchlist

All names, SSNs and accounts are fake. SSNs use the 9xx area range, which is never issued.

Usage:
  python generate_kyc_data.py --out ./landing --start 2026-10-25 --days 5
  python generate_kyc_data.py --out ./landing --bucket my-bucket --prefix landing   # also uploads (needs boto3 + AWS creds)
"""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, date

FIRST = ["Amara", "Liam", "Sofia", "Noah", "Chidi", "Elena", "Mateo", "Priya", "Jonas", "Yuki",
         "Fatima", "Andre", "Grace", "Tomas", "Ingrid", "Kofi", "Layla", "Marco", "Nadia", "Oscar"]
LAST = ["Okafor", "Martin", "Rossi", "Nguyen", "Haddad", "Kowalski", "Silva", "Patel", "Berg", "Tanaka",
        "Mensah", "Dubois", "Garcia", "Novak", "Lindqvist", "Abara", "Costa", "Ibrahim", "Fischer", "Reyes"]
CITIES = [("Tampa", "FL"), ("Miami", "FL"), ("Atlanta", "GA"), ("Austin", "TX"), ("Chicago", "IL"),
          ("New York", "NY"), ("Denver", "CO"), ("Seattle", "WA"), ("Boston", "MA"), ("Charlotte", "NC")]
STREETS = ["Oak St", "Maple Ave", "Pine Rd", "Cedar Ln", "Lake Dr", "Hill Blvd", "River Way", "Park Pl"]
MERCHANTS = [("Fresh Mart", "GROCERY"), ("Quick Fuel", "FUEL"), ("CloudHost Inc", "SOFTWARE"),
             ("Sky Airways", "TRAVEL"), ("Bella Cucina", "RESTAURANT"), ("MediCare Plus", "HEALTHCARE"),
             ("Metro Transit", "TRANSPORT"), ("Gadget Hub", "ELECTRONICS"), ("Crypto Exchange X", "CRYPTO"),
             ("Global Wire Svc", "MONEY_TRANSFER"), ("Lucky Star Casino", "GAMBLING"), ("Home Depot Co", "RETAIL")]
HIGH_RISK_COUNTRIES = ["IR", "KP", "SY", "MM"]          # used for AML scenario
NORMAL_COUNTRIES = ["US", "US", "US", "US", "CA", "GB", "DE", "MX"]
CHANNELS = ["POS", "WEB", "ATM", "MOBILE"]
TXN_TYPES = ["PURCHASE", "TRANSFER", "WITHDRAWAL", "DEPOSIT"]
RISK = ["LOW", "LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH"]
CUSTOMER_FIELDS = ["customer_id", "first_name", "last_name", "ssn", "dob", "address", "city", "state",
                   "country", "risk_rating", "kyc_status", "updated_at"]


def make_customer(i, rng, asof):
    city, st = rng.choice(CITIES)
    return {
        "customer_id": f"C{i:05d}",
        "first_name": rng.choice(FIRST),
        "last_name": rng.choice(LAST),
        "ssn": f"9{rng.randint(10, 99)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
        "dob": (date(1950, 1, 1) + timedelta(days=rng.randint(0, 18000))).isoformat(),
        "address": f"{rng.randint(10, 9999)} {rng.choice(STREETS)}",
        "city": city, "state": st, "country": "US",
        "risk_rating": rng.choice(RISK),
        "kyc_status": rng.choice(["VERIFIED", "VERIFIED", "VERIFIED", "PENDING"]),
        "updated_at": asof.strftime("%Y-%m-%d %H:%M:%S"),
    }


def write_csv(path, fields, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def rand_ts(day, rng):
    return datetime(day.year, day.month, day.day) + timedelta(seconds=rng.randint(0, 86399))


def build_txn(n, day, customers, rng, with_channel):
    cust = rng.choice(customers)
    m, cat = rng.choice(MERCHANTS)
    ttype = rng.choice(TXN_TYPES)
    amount = round(rng.lognormvariate(4.0, 1.1), 2)          # mostly small, long tail
    rec = {
        "txn_id": f"T{day.strftime('%Y%m%d')}{n:06d}",
        "customer_id": cust["customer_id"],
        "txn_ts": rand_ts(day, rng).strftime("%Y-%m-%dT%H:%M:%S"),
        "amount": amount,
        "currency": "USD",
        "txn_type": ttype,
        "merchant_name": m,
        "merchant_category": cat,
        "country": rng.choice(NORMAL_COUNTRIES),
    }
    if with_channel:
        rec["channel"] = rng.choice(CHANNELS)
    return rec


def aml_scenarios(day, customers, rng, with_channel, start_n):
    """Structuring bursts and high-risk-country activity."""
    out, n = [], start_n
    for cust in rng.sample(customers, 3):                      # structuring: 4-6 deposits of 9,000-9,900
        for _ in range(rng.randint(4, 6)):
            r = build_txn(n, day, [cust], rng, with_channel)
            r.update(txn_type="DEPOSIT", amount=round(rng.uniform(9000, 9900), 2),
                     merchant_name="Global Wire Svc", merchant_category="MONEY_TRANSFER")
            out.append(r); n += 1
    for cust in rng.sample(customers, 4):                      # high-risk-country transfers
        r = build_txn(n, day, [cust], rng, with_channel)
        r.update(txn_type="TRANSFER", amount=round(rng.uniform(2000, 25000), 2),
                 country=rng.choice(HIGH_RISK_COUNTRIES))
        out.append(r); n += 1
    return out, n


def inject_defects(rows, rng):
    """Dirty ~3% of rows: dupes, null ids, bad amounts, bad timestamps."""
    out = list(rows)
    for r in rng.sample(rows, max(1, int(len(rows) * 0.01))):          # duplicates
        out.append(dict(r))
    for r in rng.sample(out, max(1, int(len(rows) * 0.005))):          # null txn_id
        r["txn_id"] = None
    for r in rng.sample(out, max(1, int(len(rows) * 0.005))):          # negative / zero amount
        r["amount"] = rng.choice([-abs(r["amount"]), 0])
    for r in rng.sample(out, max(1, int(len(rows) * 0.005))):          # bad timestamp
        r["txn_ts"] = rng.choice(["not-a-date", "2026-13-45T99:00:00", ""])
    rng.shuffle(out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./landing")
    ap.add_argument("--start", default="2026-10-25", help="first landing date, YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--txns-per-day", type=int, default=2000)
    ap.add_argument("--late", action="store_true", help="also emit a late-arriving file for day 2")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--bucket", help="if set, upload the output to this S3 bucket")
    ap.add_argument("--prefix", default="landing")
    a = ap.parse_args()

    rng = random.Random(a.seed)
    start = datetime.strptime(a.start, "%Y-%m-%d").date()

    customers = [make_customer(i + 1, rng, datetime.combine(start, datetime.min.time())) for i in range(a.customers)]

    # Watchlist: 6 synthetic entries, 3 of which deliberately match customer names
    wl, picks = [], rng.sample(customers, 3)
    for k, c in enumerate(picks):
        wl.append({"watchlist_id": f"W{k+1:03d}", "full_name": f"{c['first_name']} {c['last_name']}",
                   "list_source": "SYNTHETIC_SANCTIONS", "country": rng.choice(HIGH_RISK_COUNTRIES),
                   "added_date": (start - timedelta(days=90)).isoformat()})
    for k in range(3, 6):
        wl.append({"watchlist_id": f"W{k+1:03d}", "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}-Zane",
                   "list_source": "SYNTHETIC_SANCTIONS", "country": rng.choice(HIGH_RISK_COUNTRIES),
                   "added_date": (start - timedelta(days=60)).isoformat()})
    write_csv(os.path.join(a.out, "watchlist", "watchlist.csv"),
              ["watchlist_id", "full_name", "list_source", "country", "added_date"], wl)

    next_cust = a.customers + 1
    for d in range(a.days):
        day = start + timedelta(days=d)
        dt = day.isoformat()
        asof = datetime.combine(day, datetime.min.time()) + timedelta(hours=2)

        # ---- customers ----
        if d == 0:
            cust_rows = customers
        else:
            cust_rows = []
            for c in rng.sample(customers, max(3, a.customers // 20)):       # changes (SCD2 fodder)
                if rng.random() < 0.5:
                    city, st = rng.choice(CITIES)
                    c.update(address=f"{rng.randint(10, 9999)} {rng.choice(STREETS)}", city=city, state=st)
                else:
                    c["risk_rating"] = rng.choice(RISK)
                c["updated_at"] = asof.strftime("%Y-%m-%d %H:%M:%S")
                cust_rows.append(dict(c))
            for _ in range(5):                                                # new customers
                nc = make_customer(next_cust, rng, asof); next_cust += 1
                customers.append(nc); cust_rows.append(dict(nc))
        write_csv(os.path.join(a.out, "customers", f"dt={dt}", "customers.csv"), CUSTOMER_FIELDS, cust_rows)

        # ---- transactions ----
        with_channel = d >= 2                                                 # schema drift from day 3
        base = [build_txn(i + 1, day, customers, rng, with_channel) for i in range(a.txns_per_day)]
        extra, _ = aml_scenarios(day, customers, rng, with_channel, a.txns_per_day + 1)
        rows = inject_defects(base + extra, rng)
        write_jsonl(os.path.join(a.out, "transactions", f"dt={dt}", "transactions_001.json"), rows)

    if a.late and a.days >= 3:
        late_day = start + timedelta(days=1)
        late = [build_txn(900000 + i, late_day, customers, rng, False) for i in range(150)]
        write_jsonl(os.path.join(a.out, "transactions", f"dt={late_day.isoformat()}", "transactions_late_002.json"), late)

    print(f"Generated {a.days} days under {os.path.abspath(a.out)}")

    if a.bucket:
        import boto3
        s3 = boto3.client("s3")
        for root, _, files in os.walk(a.out):
            for fn in files:
                p = os.path.join(root, fn)
                key = f"{a.prefix}/{os.path.relpath(p, a.out)}".replace(os.sep, "/")
                s3.upload_file(p, a.bucket, key)
        print(f"Uploaded to s3://{a.bucket}/{a.prefix}/")


if __name__ == "__main__":
    main()
