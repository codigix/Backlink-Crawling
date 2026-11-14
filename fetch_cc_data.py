#!/usr/bin/env python3
"""One-command Common Crawl setup. Run once after pulling the branch:

    python fetch_cc_data.py

It installs what is missing (kaggle package, Java 21), asks for your Kaggle token if none is set, downloads the
cc-webgraph jar and the domain graph (~10 GB, resumable: rerun after an interruption), then checks a lookup works.
Data goes to CC_DATA_DIR (default ~/cc_data). Safe to rerun: finished files are skipped.
Options: --release NAME, --no-tool, --ref OWNER/SLUG, --list (show your Kaggle datasets).
"""
import argparse, getpass, glob, os, platform, shutil, subprocess, sys, tarfile
from pathlib import Path

OWNER = os.environ.get("KAGGLE_USERNAME", "chetankharade127750")  # only used if --ref is not given
DATA = Path(os.environ.get("CC_DATA_DIR", "~/cc_data")).expanduser()
EXTS = (".graph", "-t.graph", ".properties", "-t.properties", ".iepm")
TOKEN_FILE = Path("~/.kaggle/access_token").expanduser()


def run(*cmd):
    return subprocess.run(cmd).returncode == 0


def have_java():
    return bool(shutil.which("java") or glob.glob("C:/Program Files/Microsoft/jdk-21*/bin/java.exe"))


def ensure_java():
    if have_java():
        return
    print("Java 21 not found; installing ...")
    osname = platform.system()
    if osname == "Windows":
        run("winget", "install", "-e", "--id", "Microsoft.OpenJDK.21", "--accept-package-agreements", "--accept-source-agreements")
    elif osname == "Darwin":
        run("brew", "install", "openjdk@21")
    elif shutil.which("apt-get"):
        sudo = [] if os.geteuid() == 0 else ["sudo"]
        run(*sudo, "apt-get", "install", "-y", "openjdk-21-jdk-headless")
    if not have_java():
        sys.exit("Could not install Java automatically. Install JDK 21 (not just a JRE), then rerun this command.")


def ensure_kaggle():
    try:
        import kaggle  # noqa: F401
    except (ImportError, OSError):  # the kaggle package authenticates at import time and raises when no token is set
        if not run(sys.executable, "-m", "pip", "install", "-q", "kaggle"):
            sys.exit("pip install kaggle failed")


def ensure_token():
    if os.environ.get("KAGGLE_API_TOKEN") or TOKEN_FILE.exists():
        return
    if not sys.stdin.isatty():
        sys.exit("No Kaggle token: set KAGGLE_API_TOKEN (create one at https://www.kaggle.com/settings/api) and rerun.")
    print("A Kaggle API token is needed once to download the data (https://www.kaggle.com/settings/api -> Generate New Token).")
    token = getpass.getpass("Paste token (input hidden): ").strip()
    if not token:
        sys.exit("No token given.")
    os.environ["KAGGLE_API_TOKEN"] = token
    if input(f"Save it to {TOKEN_FILE} for next time? [y/N] ").strip().lower() == "y":
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(token)
        TOKEN_FILE.chmod(0o600)


def api():
    from kaggle.api.kaggle_api_extended import KaggleApi  # reads KAGGLE_API_TOKEN / ~/.kaggle/access_token itself
    k = KaggleApi()
    k.authenticate()
    return k


def fetch(slug, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    api().dataset_download_files(slug if "/" in slug else f"{OWNER}/{slug}", path=str(out_dir), unzip=True, quiet=False)


def fetch_files(slug, out_dir):
    """File-by-file: the bulk zip 404s on big datasets; files of the right size are skipped, so a rerun resumes."""
    k = api()
    ref = slug if "/" in slug else f"{OWNER}/{slug}"
    for f in k.dataset_list_files(ref).files:
        dest = out_dir / f.name
        if dest.exists() and dest.stat().st_size == f.total_bytes:
            continue
        print(f"{f.name} ({f.total_bytes / 1e9:.2f} GB)")
        k.dataset_download_file(ref, f.name, path=str(out_dir), force=True, quiet=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", default="cc-main-2026-aug-sep-oct")
    ap.add_argument("--no-tool", action="store_true", help="skip the cc-webgraph jar download")
    ap.add_argument("--list", action="store_true", help="print the exact names of your Kaggle datasets")
    ap.add_argument("--ref", help="exact <owner>/<slug> of the release dataset, from --list")
    a = ap.parse_args()

    g = str(DATA / f"{a.release}-domain")
    jar_glob = str(DATA / "cc-webgraph/target/*jar-with-dependencies.jar")
    need_tool = not a.no_tool and not glob.glob(jar_glob)
    need_data = not all(Path(g + e).exists() for e in EXTS)
    ensure_java()
    if a.list or need_tool or need_data:
        ensure_kaggle()
        ensure_token()
    if a.list:
        for d in api().dataset_list(mine=True):
            print(d.ref)
        return
    DATA.mkdir(parents=True, exist_ok=True)
    if need_data and shutil.disk_usage(DATA).free < 25 * 1024**3:
        sys.exit("Less than 25 GB free; free some space first.")

    if need_tool:
        tmp = DATA / "_tool"
        fetch("cc-webgraph-tool", tmp)
        tars = list(tmp.rglob("cc-webgraph-built.tar.gz"))
        if tars:
            with tarfile.open(tars[0]) as t:
                t.extractall(DATA, filter="data")
        elif (tmp / "cc-webgraph").is_dir():  # Kaggle already unpacked it
            shutil.copytree(tmp / "cc-webgraph", DATA / "cc-webgraph", dirs_exist_ok=True)
        else:
            sys.exit("cc-webgraph not found in tool dataset.")
        shutil.rmtree(tmp, ignore_errors=True)

    if need_data:
        fetch_files(a.ref or "cc-webgraph-" + a.release.replace("cc-main-", ""), DATA)
        if not all(Path(g + e).exists() for e in EXTS):
            sys.exit("Release files still missing after download; rerun this command to resume.")
    Path(g + ".vertexmap.done").touch()

    # end-to-end proof that a lookup works on this machine (~15 s)
    sys.path.insert(0, str(Path(__file__).parent))
    import cc_lookup
    try:
        found = cc_lookup.referring_domains("github.com", a.release)
        assert found and len(found) > 1000
    except Exception as e:  # noqa: BLE001
        sys.exit(f"Downloaded, but the test lookup failed: {e}")
    print(f"Ready: Common Crawl lookups work ({a.release}, {DATA}).")


if __name__ == "__main__":
    main()
