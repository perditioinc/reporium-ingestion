# Enrichment Prompt V2

## 16-Category Fixed Taxonomy
Every repo MUST be assigned exactly ONE primary_category from this list:
agents, rag-retrieval, llm-serving, fine-tuning, evaluation, orchestration, vector-databases, observability, security-safety, code-generation, data-processing, computer-vision, nlp-text, speech-audio, generative-media, infrastructure

## Prompt Template
Given a GitHub repository with this context:
- Name: {name}
- Description: {description}
- README (first 2000 chars): {readme}
- Languages: {languages}
- Topics: {topics}
- Dependencies: {dependencies}

Respond with valid JSON only:
{
  "primary_category": "<one of the 16 categories>",
  "secondary_categories": ["<up to 3 additional categories>"],
  "readme_summary": "<2-3 sentence summary>",
  "problem_solved": "<what problem this solves>",
  "integration_tags": ["<relevant tags>"],
  "quality_signals": {
    "has_tests": <bool>,
    "has_ci": <bool>,
    "has_docs": <bool>,
    "maintenance_status": "<active|maintained|stale|archived>",
    "star_tier": "<mega|high|mid|low|micro>"
  },
  "ai_dev_skills": ["<which AI dev coverage skills this repo addresses>"]
}

## Validation Rules
- primary_category MUST be from the 16-category list (reject if not)
- secondary_categories max 3, all from the 16-category list
- quality_signals is required
- ai_dev_skills maps to the coverage badges on the dashboard

## Additive Pattern
- NEVER delete existing enrichment data before new data is verified
- Write to NEW columns or UPDATE only NULL fields first
- Verify counts match expectations before bulk UPDATE of existing data
- If enrichment fails midway, already-enriched repos keep their data
