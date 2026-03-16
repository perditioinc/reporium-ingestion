from pydantic import BaseModel
from .trends import TrendSnapshot


class Gap(BaseModel):
    category: str
    description: str
    severity: str  # 'high', 'medium', 'low'
    repo_count: int
    suggestion: str


# Minimum repo counts expected per category in a healthy portfolio
EXPECTED_MINIMUMS: dict[str, int] = {
    'Foundation Models': 10,
    'AI Agents': 8,
    'RAG & Retrieval': 6,
    'Model Training': 5,
    'Evals & Benchmarking': 4,
    'Observability & Monitoring': 4,
    'Inference & Serving': 4,
    'Generative Media': 3,
    'Computer Vision': 3,
    'MLOps & Infrastructure': 5,
    'Dev Tools & Automation': 5,
    'Learning Resources': 3,
}


def detect_gaps(snapshot: TrendSnapshot) -> list[Gap]:
    """Detect coverage gaps based on current snapshot vs expected minimums."""
    gaps: list[Gap] = []
    cat_counts = snapshot.category_counts

    for category, expected in EXPECTED_MINIMUMS.items():
        actual = cat_counts.get(category, 0)
        if actual < expected:
            deficit = expected - actual
            if deficit >= expected * 0.75:
                severity = 'high'
            elif deficit >= expected * 0.4:
                severity = 'medium'
            else:
                severity = 'low'

            gaps.append(Gap(
                category=category,
                description=f'Only {actual} repos in {category} (expected {expected}+)',
                severity=severity,
                repo_count=actual,
                suggestion=f'Add {deficit} more repos covering {category}',
            ))

    return sorted(gaps, key=lambda g: ('high', 'medium', 'low').index(g.severity))
