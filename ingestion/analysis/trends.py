from datetime import datetime, timezone
from pydantic import BaseModel


class TrendSnapshot(BaseModel):
    captured_at: str
    total_repos: int
    top_tags: list[str]
    top_categories: list[str]
    active_repos: int
    new_repos_last_30d: int
    tag_counts: dict[str, int]
    category_counts: dict[str, int]


def build_trend_snapshot(repos: list[dict]) -> TrendSnapshot:
    """Build a trend snapshot from the current enriched repo list."""
    now = datetime.now(timezone.utc).isoformat()

    tag_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    active = 0
    new_last_30d = 0
    cutoff_30d = datetime.now(timezone.utc).timestamp() - (30 * 86400)

    for repo in repos:
        for tag in repo.get('tags', []):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

        for cat in repo.get('categories', []):
            name = cat.get('category_name', '') if isinstance(cat, dict) else cat
            if name:
                category_counts[name] = category_counts.get(name, 0) + 1

        if 'Active' in repo.get('tags', []):
            active += 1

        updated_at = repo.get('github_updated_at', '')
        if updated_at:
            try:
                ts = datetime.fromisoformat(updated_at.replace('Z', '+00:00')).timestamp()
                if ts > cutoff_30d:
                    new_last_30d += 1
            except Exception:
                pass

    top_tags = sorted(tag_counts, key=lambda t: tag_counts[t], reverse=True)[:20]
    top_categories = sorted(category_counts, key=lambda c: category_counts[c], reverse=True)[:10]

    return TrendSnapshot(
        captured_at=now,
        total_repos=len(repos),
        top_tags=top_tags,
        top_categories=top_categories,
        active_repos=active,
        new_repos_last_30d=new_last_30d,
        tag_counts=tag_counts,
        category_counts=category_counts,
    )
