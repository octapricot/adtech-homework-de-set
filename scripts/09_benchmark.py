# PURPOSE: Measures response times with and without Redis caching and present results in a comparison table

import requests
import time
import json
import redis
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL   = "http://127.0.0.1:8000"
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

ENDPOINTS = [
    {
        "name":     "Campaign Performance",
        "url":      f"{BASE_URL}/campaign/191/performance",
        "cache_key": "campaign:191:performance"
    },
    {
        "name":     "Advertiser Spending",
        "url":      f"{BASE_URL}/advertiser/1/spending",
        "cache_key": "advertiser:1:spending"
    },
    {
        "name":     "User Engagements",
        "url":      f"{BASE_URL}/user/59472/engagements",
        "cache_key": "user:59472:engagements"
    }
]

RUNS = 5  


def clear_cache(keys):
    """Delets specific keys from Redis to force a cache MISS"""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    for key in keys:
        r.delete(key)


def measure(url, runs=RUNS):
    """Calls an endpoint multiple times and returns average, min and max response times in milliseconds."""
    times = []
    for _ in range(runs):
        start    = time.perf_counter()
        response = requests.get(url)
        elapsed  = (time.perf_counter() - start) * 1000
        times.append(elapsed)
        time.sleep(0.1)
    return {
        "avg_ms": round(sum(times) / len(times), 1),
        "min_ms": round(min(times), 1),
        "max_ms": round(max(times), 1)
    }


def run_benchmark():
    results = []

    for endpoint in ENDPOINTS:
        print(f"\nBenchmarking: {endpoint['name']}")

        print("  Running WITHOUT cache (clearing Redis before each call) ...")
        no_cache_times = []
        for i in range(RUNS):
            clear_cache([endpoint["cache_key"]])
            start    = time.perf_counter()
            requests.get(endpoint["url"])
            elapsed  = (time.perf_counter() - start) * 1000
            no_cache_times.append(elapsed)
            print(f"    Run {i+1}: {elapsed:.1f}ms (MISS)", end="\r")
            time.sleep(0.2)

        no_cache = {
            "avg_ms": round(sum(no_cache_times) / len(no_cache_times), 1),
            "min_ms": round(min(no_cache_times), 1),
            "max_ms": round(max(no_cache_times), 1)
        }
        print(f"  Without cache: avg={no_cache['avg_ms']}ms")

        print("  Running WITH cache ...")
        requests.get(endpoint["url"]) 
        time.sleep(0.2)

        cached_times = []
        for i in range(RUNS):
            start   = time.perf_counter()
            requests.get(endpoint["url"])
            elapsed = (time.perf_counter() - start) * 1000
            cached_times.append(elapsed)
            print(f"    Run {i+1}: {elapsed:.1f}ms (HIT)", end="\r")
            time.sleep(0.1)

        with_cache = {
            "avg_ms": round(sum(cached_times) / len(cached_times), 1),
            "min_ms": round(min(cached_times), 1),
            "max_ms": round(max(cached_times), 1)
        }
        print(f"  With cache:    avg={with_cache['avg_ms']}ms")

        speedup = round(no_cache["avg_ms"] / with_cache["avg_ms"], 1)

        results.append({
            "endpoint":   endpoint["name"],
            "without_cache_avg_ms": no_cache["avg_ms"],
            "without_cache_min_ms": no_cache["min_ms"],
            "without_cache_max_ms": no_cache["max_ms"],
            "with_cache_avg_ms":    with_cache["avg_ms"],
            "with_cache_min_ms":    with_cache["min_ms"],
            "with_cache_max_ms":    with_cache["max_ms"],
            "speedup_x":            speedup
        })

    return results


def print_table(results):
    print("\n" + "=" * 80)
    print("BENCHMARKING RESULTS: Response time comparison (in ms)")
    print("=" * 80)
    print(f"{'Endpoint':<25} {'No Cache avg':>13} {'Cached avg':>11} {'Speedup':>9}")
    print("-" * 80)
    for r in results:
        print(
            f"{r['endpoint']:<25} "
            f"{r['without_cache_avg_ms']:>10.1f}ms "
            f"{r['with_cache_avg_ms']:>8.1f}ms "
            f"{r['speedup_x']:>8.1f}x"
        )
    print("=" * 80)


def main():
    print("Starting benchmark ...")
    print(f"Each endpoint will be called {RUNS} times per scenario\n")

    results = run_benchmark()
    print_table(results)

    os.makedirs("reports", exist_ok=True)
    output_path = "reports/benchmark_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()