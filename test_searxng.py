import json
import requests

res = requests.get(
    "http://127.0.0.1:8888/search",
    params={"q": "artificial intelligence startups", "format": "json", "pageno": 1},
    timeout=20,
)
res.raise_for_status()
data = res.json()
results = data.get("results", [])
print(f"SUCCESS: Retrieved {len(results)} results.")
print(json.dumps(results[:2], indent=2))
