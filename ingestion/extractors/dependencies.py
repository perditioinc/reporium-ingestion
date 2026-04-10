"""
Extracts dependency information from repos without using any AI.
This is free — only uses GitHub API with existing GH_TOKEN.
Cost: $0

For each repo, fetches requirements.txt / pyproject.toml / package.json from GitHub,
parses package names (not versions), and stores as JSON array in the dependencies column.

If no dependency file is found: stores [] (empty array) — marks it as "checked, no deps found".
If GitHub API returns 404 for all files: stores [].
If GitHub API rate limits: pauses and retries once, then skips.
"""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Optional

import httpx
import psycopg2

logger = logging.getLogger(__name__)

# Files to check, in priority order (stop at first found)
DEPENDENCY_FILES = [
    "requirements.txt",
    "pyproject.toml",
    "package.json",
    "setup.py",
    "go.mod",
    "Cargo.toml",
]


@dataclass
class ExtractionResult:
    repo_id: str
    repo_name: str
    dependencies: list[str]
    source_file: Optional[str]
    ecosystem: Optional[str] = None
    error: Optional[str] = None


def parse_requirements_txt(content: str) -> list[str]:
    """Parse package names from requirements.txt content."""
    deps = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        # Handle: package==1.0, package>=1.0, package[extra]>=1.0, package @ git+...
        match = re.match(r"^([a-zA-Z0-9_-]+)", line)
        if match:
            deps.append(match.group(1).lower())
    return deps


def parse_pyproject_toml(content: str) -> list[str]:
    """Parse package names from pyproject.toml [project.dependencies]."""
    deps = []
    in_deps = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped in ("[project]", "[tool.poetry.dependencies]"):
            continue
        if stripped == "dependencies = [" or stripped.startswith("dependencies = ["):
            in_deps = True
            # Check for inline list
            if "[" in stripped and "]" in stripped:
                inner = stripped.split("[", 1)[1].rsplit("]", 1)[0]
                for item in inner.split(","):
                    item = item.strip().strip('"').strip("'")
                    match = re.match(r"^([a-zA-Z0-9_-]+)", item)
                    if match:
                        deps.append(match.group(1).lower())
                in_deps = False
            continue
        if in_deps:
            if stripped == "]":
                in_deps = False
                continue
            item = stripped.strip(",").strip('"').strip("'")
            match = re.match(r"^([a-zA-Z0-9_-]+)", item)
            if match:
                deps.append(match.group(1).lower())
    return deps


def parse_package_json(content: str) -> list[str]:
    """Parse package names from package.json dependencies + devDependencies."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return []
    deps = set()
    for key in ("dependencies", "devDependencies", "peerDependencies"):
        section = data.get(key, {})
        if isinstance(section, dict):
            deps.update(section.keys())
    return sorted(deps)


def parse_setup_py(content: str) -> list[str]:
    """Extract package names from setup.py install_requires (best effort)."""
    deps = []
    match = re.search(r"install_requires\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if match:
        for item in match.group(1).split(","):
            item = item.strip().strip('"').strip("'")
            pkg_match = re.match(r"^([a-zA-Z0-9_-]+)", item)
            if pkg_match:
                deps.append(pkg_match.group(1).lower())
    return deps


def parse_go_mod(content: str) -> list[str]:
    """Parse module paths from go.mod require blocks."""
    deps = []
    in_require = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith('require ('):
            in_require = True
            continue
        if in_require:
            if stripped == ')':
                in_require = False
                continue
            # Format: github.com/some/pkg v1.2.3
            parts = stripped.split()
            if parts and not parts[0].startswith('//'):
                deps.append(parts[0])
        elif stripped.startswith('require ') and not stripped.startswith('require ('):
            # Single-line: require github.com/some/pkg v1.2.3
            parts = stripped[8:].split()
            if parts:
                deps.append(parts[0])
    return deps


def parse_cargo_toml(content: str) -> list[str]:
    """Parse crate names from Cargo.toml [dependencies] section."""
    deps = []
    in_deps = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped in ('[dependencies]', '[dev-dependencies]', '[build-dependencies]'):
            in_deps = True
            continue
        if stripped.startswith('[') and stripped != '[dependencies]':
            in_deps = False
            continue
        if in_deps and '=' in stripped and not stripped.startswith('#'):
            name = stripped.split('=')[0].strip()
            if name:
                deps.append(name)
    return deps


PARSERS = {
    "requirements.txt": parse_requirements_txt,
    "pyproject.toml": parse_pyproject_toml,
    "package.json": parse_package_json,
    "setup.py": parse_setup_py,
    "go.mod": parse_go_mod,
    "Cargo.toml": parse_cargo_toml,
}

# Map dependency file names to package ecosystem identifiers.
FILE_TO_ECOSYSTEM = {
    "requirements.txt": "pypi",
    "pyproject.toml": "pypi",
    "setup.py": "pypi",
    "package.json": "npm",
    "go.mod": "go",
    "Cargo.toml": "cargo",
}


async def fetch_file_content(
    client: httpx.AsyncClient,
    owner: str,
    repo: str,
    filepath: str,
    token: str,
) -> Optional[str]:
    """Fetch a file from GitHub API. Returns content or None if not found."""
    # Use the upstream repo if it's a fork (forked_from)
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{filepath}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.raw"}
    try:
        resp = await client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 404:
            return None
        if resp.status_code == 403:
            # Rate limited
            remaining = int(resp.headers.get("x-ratelimit-remaining", "0"))
            if remaining == 0:
                logger.warning("Rate limited on %s/%s/%s", owner, repo, filepath)
                return None
        return None
    except Exception as exc:
        logger.warning("Error fetching %s/%s/%s: %s", owner, repo, filepath, exc)
        return None


async def extract_dependencies_for_repo(
    client: httpx.AsyncClient,
    repo_id: str,
    owner: str,
    repo_name: str,
    forked_from: Optional[str],
    token: str,
) -> ExtractionResult:
    """
    Extract dependencies for a single repo.
    Tries upstream repo if it's a fork, falls back to fork itself.
    """
    # If it's a fork, try the upstream first (more likely to have deps)
    targets = []
    if forked_from:
        parts = forked_from.split("/")
        if len(parts) == 2:
            targets.append((parts[0], parts[1]))
    targets.append((owner, repo_name))

    for target_owner, target_repo in targets:
        for filepath in DEPENDENCY_FILES:
            content = await fetch_file_content(client, target_owner, target_repo, filepath, token)
            if content:
                parser = PARSERS.get(filepath)
                if parser:
                    deps = parser(content)
                    return ExtractionResult(
                        repo_id=repo_id,
                        repo_name=f"{owner}/{repo_name}",
                        dependencies=deps,
                        source_file=f"{target_owner}/{target_repo}/{filepath}",
                        ecosystem=FILE_TO_ECOSYSTEM.get(filepath, "unknown"),
                    )

    # No dependency file found — return empty (checked, no deps)
    return ExtractionResult(
        repo_id=repo_id,
        repo_name=f"{owner}/{repo_name}",
        dependencies=[],
        source_file=None,
    )


async def run_dependency_extraction(db_url: str, gh_token: str) -> dict:
    """
    Main entry point: extract dependencies for all repos not yet scanned.

    Writes to the ``repo_dependencies`` table (migration 029).  A repo is
    considered "scanned" once it has at least one row in that table.  Repos
    with no dependency file get a sentinel row (package_name='__none__') so
    they are not rescanned on the next run.
    """
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Find repos that have never been scanned for dependencies.
    # A repo is "scanned" once it has at least one row in repo_dependencies.
    cur.execute("""
        SELECT r.id, r.name, r.owner, r.forked_from
        FROM repos r
        LEFT JOIN repo_dependencies rd ON r.id = rd.repo_id
        WHERE rd.repo_id IS NULL
        ORDER BY r.name;
    """)
    repos = cur.fetchall()
    total = len(repos)
    logger.info("Found %d repos needing dependency scan", total)

    if total == 0:
        conn.close()
        return {"total": 0, "processed": 0, "with_deps": 0, "no_deps": 0, "errors": 0}

    processed = 0
    with_deps = 0
    no_deps = 0
    errors = 0
    semaphore = asyncio.Semaphore(2)  # Max 2 concurrent requests

    async with httpx.AsyncClient() as client:
        for i, (repo_id, name, owner, forked_from) in enumerate(repos):
            async with semaphore:
                result = await extract_dependencies_for_repo(
                    client, str(repo_id), owner, name, forked_from, gh_token
                )

                if result.error:
                    errors += 1
                    logger.warning("Error for %s: %s", result.repo_name, result.error)
                else:
                    if result.dependencies:
                        # Insert each dependency into repo_dependencies
                        for pkg in result.dependencies:
                            cur.execute(
                                """INSERT INTO repo_dependencies
                                       (id, repo_id, package_name, package_ecosystem, is_direct)
                                   VALUES (gen_random_uuid(), %s, %s, %s, true)
                                   ON CONFLICT (repo_id, package_name, package_ecosystem)
                                   DO NOTHING""",
                                (str(repo_id), pkg, result.ecosystem),
                            )
                        with_deps += 1
                    else:
                        # Sentinel row: marks repo as scanned with zero deps
                        cur.execute(
                            """INSERT INTO repo_dependencies
                                   (id, repo_id, package_name, package_ecosystem, is_direct)
                               VALUES (gen_random_uuid(), %s, '__none__', '__sentinel__', false)
                               ON CONFLICT (repo_id, package_name, package_ecosystem)
                               DO NOTHING""",
                            (str(repo_id),),
                        )
                        no_deps += 1

                    conn.commit()
                    processed += 1

                # Progress logging every 50 repos
                if (i + 1) % 50 == 0:
                    logger.info(
                        "Progress: %d/%d (with deps: %d, no deps: %d, errors: %d)",
                        i + 1, total, with_deps, no_deps, errors,
                    )

                # Rate limit: 500ms delay between requests
                await asyncio.sleep(0.5)

    conn.close()

    summary = {
        "total": total,
        "processed": processed,
        "with_deps": with_deps,
        "no_deps": no_deps,
        "errors": errors,
    }
    logger.info("Dependency extraction complete: %s", summary)
    return summary
