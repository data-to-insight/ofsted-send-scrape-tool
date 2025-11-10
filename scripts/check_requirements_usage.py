#!/usr/bin/env python3


import ast
import sys
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REQ_FILE = PROJECT_ROOT / "requirements.txt"
EXCLUDE_DIRS = {
    ".git", ".github", ".venv", "venv", "env", "__pycache__", "site-packages",
    "dist", "build", "docs", "data", "assets", "node_modules"
}

# Map distribution name -> list of top level import names commonly used
KNOWN_IMPORTS = {
    "tabula-py":        ["tabula"],
    "textblob":         ["textblob"],
    "nltk":             ["nltk"],
    "xlsxwriter":       ["xlsxwriter"],
    "PyPDF2":           ["PyPDF2"],
    "PyMuPDF":          ["fitz", "pymupdf"],
    "requests":         ["requests"],
    "beautifulsoup4":   ["bs4"],
    "GitPython":        ["git"],
    "scipy":            ["scipy"],
    # add here if project uses others
}

def canonicalize_name(name: str) -> str:
    # similar to packaging.utils.canonicalize_name, without dependency
    return re.sub(r"[-_.]+", "-", name).lower().strip()

def parse_requirements_lines(path: Path):
    """Return tuple: (listed_names_set, metadata_list)
    metadata_list keeps original lines and parsed canonical dist names where present.
    """
    listed = set()
    lines_meta = []
    req_re = re.compile(r"^\s*([A-Za-z0-9_.\-]+)")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            lines_meta.append(("comment_or_blank", raw, None))
            continue
        m = req_re.match(line)
        if not m:
            lines_meta.append(("other", raw, None))
            continue
        dist = canonicalize_name(m.group(1))
        listed.add(dist)
        lines_meta.append(("requirement", raw, dist))
    return listed, lines_meta

def iter_python_files(root: Path):
    for p in root.rglob("*.py"):
        rel_parts = p.relative_to(root).parts
        if any(part in EXCLUDE_DIRS for part in rel_parts):
            continue
        yield p

def collect_local_packages(root: Path):
    """Top level package names to treat as local, exclude from third party detection."""
    locals_set = set()
    for d in root.iterdir():
        if d.is_dir():
            init_py = d / "__init__.py"
            if init_py.exists():
                locals_set.add(d.name.split(".")[0])
    # also include top level scripts as local modules
    for f in root.glob("*.py"):
        locals_set.add(f.stem)
    return locals_set

def collect_imports(pyfile: Path):
    try:
        tree = ast.parse(pyfile.read_text(encoding="utf-8"), filename=str(pyfile))
    except Exception:
        return set()
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                top = n.name.split(".", 1)[0]
                used.add(top)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                # relative import, treat as local
                continue
            if node.module:
                top = node.module.split(".", 1)[0]
                used.add(top)
    return used

def build_reverse_map():
    rev = {}
    for dist, tops in KNOWN_IMPORTS.items():
        for top in tops:
            rev.setdefault(top, set()).add(canonicalize_name(dist))
    return rev

def main():
    root = PROJECT_ROOT
    if not REQ_FILE.exists():
        print(f"requirements.txt not found at {REQ_FILE}", file=sys.stderr)
        sys.exit(2)

    listed, lines_meta = parse_requirements_lines(REQ_FILE)
    reverse_map = build_reverse_map()
    stdlib = set(getattr(sys, "stdlib_module_names", set()))  # available on 3.10+

    local_pkgs = collect_local_packages(root)

    imports = set()
    for f in iter_python_files(root):
        imports |= collect_imports(f)

    # Classify imports
    third_party_imports = set()
    unknown_imports = set()
    import_to_dists = {}

    for imp in sorted(imports):
        if imp in stdlib:
            continue
        if imp in local_pkgs:
            continue
        dists = reverse_map.get(imp)
        if dists:
            import_to_dists[imp] = dists
            third_party_imports.add(imp)
        else:
            # try heuristic where dist name equals import name
            guessed = canonicalize_name(imp)
            import_to_dists[imp] = {guessed}
            third_party_imports.add(imp)
            # mark as unknown if not in known map, this may be stdlib alias or missing mapping
            if guessed not in listed:
                unknown_imports.add(imp)

    # Compute used distributions by mapping known imports
    used_dists = set()
    for im, dists in import_to_dists.items():
        for d in dists:
            used_dists.add(d)

    # Of used distributions, keep only those that look like real third party packages
    # avoids flagging local packages that slipped through
    # consider real if either it is listed already, or it is in known map values
    known_dist_names = {canonicalize_name(k) for k in KNOWN_IMPORTS.keys()}
    used_real = {d for d in used_dists if d in listed or d in known_dist_names}

    unused_listed = sorted(listed - used_real)
    missing_direct = sorted(used_real - listed)

    print("\n=== Import scan summary ===")
    print(f"Python files scanned: {len(list(iter_python_files(root)))}")
    print(f"Total unique imports found: {len(imports)}")
    print(f"Third party import roots detected: {sorted(third_party_imports)}")
    print()

    if unused_listed:
        print("Possibly unused in code, listed in requirements.txt:")
        for d in unused_listed:
            print(f"  - {d}")
    else:
        print("No obviously unused packages from requirements.txt")

    if missing_direct:
        print("\nDirect imports in code that are not in requirements.txt:")
        for d in missing_direct:
            print(f"  - {d}")
        print("These may be satisfied as transitive deps, but best practice is to list direct imports you use.")
    else:
        print("\nNo missing direct packages based on import scan")

    unknown_third_party = sorted(
        {u for u in unknown_imports if canonicalize_name(u) not in listed}
    )
    if unknown_third_party:
        print("\nUnknown imports not mapped to a distribution name:")
        for u in unknown_third_party:
            print(f"  - {u}  (add to KNOWN_IMPORTS if this is third party)")
        print("Some of these could be stdlib modules on your Python version, or local modules.")

    # trimmed requirements file based on used_real
    out_lines = []
    kept = set()
    for kind, raw, dist in lines_meta:
        if kind != "requirement":
            out_lines.append(raw)
            continue
        if dist in used_real:
            out_lines.append(raw)
            kept.add(dist)
        else:
            # skip unused requirement lines
            pass

    out_path = REQ_FILE.with_name("requirements.used.txt")
    out_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    print(f"\nWrote trimmed requirements to {out_path}")
    if unused_listed:
        print("Review manually before replacing requirements.txt")

if __name__ == "__main__":
    main()
