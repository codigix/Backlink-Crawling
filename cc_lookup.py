"""Common Crawl domain-graph lookup: which domains link to a given domain (runs BacklinkQuery.java through the cc-webgraph jar).
Data comes from fetch_cc_data.py. Env: CC_DATA_DIR (default ~/cc_data), CC_RELEASE (default cc-main-2026-aug-sep-oct)."""
import glob
import os
import shutil
import subprocess
from pathlib import Path

DATA = Path(os.environ.get("CC_DATA_DIR", "~/cc_data")).expanduser()
RELEASE = os.environ.get("CC_RELEASE", "cc-main-2026-aug-sep-oct")
HERE = Path(__file__).parent


def query(graph: str, domain: str) -> list[str] | None:
    """Referring domains of `domain`, or None if it is not in the graph. Raises RuntimeError if Java/data is missing."""
    jar = glob.glob(str(DATA / "cc-webgraph/target/*jar-with-dependencies.jar"))
    if not jar:
        raise RuntimeError("cc-webgraph jar missing: run fetch_cc_data.py --tool")
    # PATH may lack java right after winget install; fall back to the default install dir
    java = shutil.which("java") or next(iter(sorted(glob.glob("C:/Program Files/Microsoft/jdk-21*/bin/java.exe"))), None)
    if not java:
        raise RuntimeError("java not found: install JDK 21 (winget install Microsoft.OpenJDK.21)")
    if not Path(graph + ".iepm").exists():
        raise RuntimeError(f"graph data missing at {graph}: run fetch_cc_data.py")
    r = subprocess.run([java, "-Xmx4g", "-cp", jar[0], str(HERE / "BacklinkQuery.java"), graph, domain],
                       text=True, capture_output=True, timeout=600)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-1500:])
    lines = r.stdout.splitlines()
    return None if lines[-1] == "NOT_IN_GRAPH" else lines[:-1]  # last line is "COUNT n"


def referring_domains(domain: str, release: str = RELEASE) -> list[str] | None:
    return query((DATA / f"{release}-domain").as_posix(), domain)
