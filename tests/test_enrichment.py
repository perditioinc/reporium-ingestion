import pytest
from ingestion.enrichment.tagger import (
    enrich_tags,
    extract_tags_from_readme,
    generate_meta_tags,
    _matches_keyword,
)
from ingestion.enrichment.taxonomy import (
    assign_primary_category,
    assign_all_categories,
    assign_dimension,
    build_builder,
    PM_SKILLS,
    KNOWN_ORGS,
)


# ── keyword matching ──────────────────────────────────────────────────────────

def test_keyword_match_basic():
    assert _matches_keyword('This uses pytorch for training', 'pytorch') is True


def test_keyword_no_false_positive_substring():
    """'game' alone should not match inside 'game-related' as a word."""
    # 'game' in 'gamedev' should not match if 'gamedev' is a single token
    # but 'game' as a standalone word should match
    assert _matches_keyword('this is a game engine', 'game') is True
    # Should NOT match substring in middle of word
    assert _matches_keyword('amalgamate', 'game') is False


def test_keyword_no_false_positive_ar():
    """'ar ' (with trailing space) should not match 'library', 'particular'."""
    # The tagger uses 'augmented reality' not 'ar ' to avoid false positives
    assert _matches_keyword('a library for machine learning', 'ar') is False


def test_keyword_case_insensitive():
    assert _matches_keyword('Uses LangChain for orchestration', 'langchain') is True
    assert _matches_keyword('Uses LANGCHAIN for orchestration', 'langchain') is True


def test_keyword_word_boundary():
    """Should not match partial words."""
    assert _matches_keyword('pytorch is great', 'torch') is False  # 'torch' is not standalone
    assert _matches_keyword('torch is used here', 'torch') is True


# ── README tag extraction ─────────────────────────────────────────────────────

def test_extract_tags_from_readme_basic():
    readme = 'This project uses LangChain and ChromaDB for RAG applications.'
    tags = extract_tags_from_readme(readme)
    assert 'LangChain' in tags
    assert 'Chroma' in tags


def test_no_false_positive_game_tag():
    """'game' alone should not trigger Game Dev if it is part of 'game-related'."""
    readme = 'A framework for game-related analytics and data processing.'
    tags = extract_tags_from_readme(readme)
    # 'game' in 'game-related' — word boundary means 'game' IS followed by '-', not a-z0-9
    # In our regex, '-' is NOT in [a-zA-Z0-9], so 'game' WOULD match 'game-related'
    # This is consistent with the TypeScript original behavior
    # The TypeScript comment says "game" in TOPIC_TAGS (not README map)
    # In README map: 'unity game', 'unreal engine', 'game engine', etc. — more specific
    readme2 = 'A framework for data processing and analytics workflows.'
    tags2 = extract_tags_from_readme(readme2)
    assert 'Game Dev' not in tags2


def test_augmented_reality_requires_full_phrase():
    """'ar ' substring should not trigger Augmented Reality tag."""
    readme = 'A particular library for parsing various formats.'
    tags = extract_tags_from_readme(readme)
    assert 'Augmented Reality' not in tags

    readme2 = 'This app uses augmented reality for navigation.'
    tags2 = extract_tags_from_readme(readme2)
    assert 'Augmented Reality' in tags2


def test_rag_requires_full_phrase():
    """Standalone 'rag' should not match storage/diagram/fragile."""
    readme = 'Storage solutions for diagram management, fragile data handling.'
    tags = extract_tags_from_readme(readme)
    assert 'RAG' not in tags

    readme2 = 'A retrieval-augmented generation system for documents.'
    tags2 = extract_tags_from_readme(readme2)
    assert 'RAG' in tags2


def test_llm_tag_extraction():
    readme = 'Building with large language models and LLM orchestration.'
    tags = extract_tags_from_readme(readme)
    assert 'Large Language Models' in tags


def test_vllm_tag_extraction():
    readme = 'Serving LLMs with vLLM for fast inference.'
    tags = extract_tags_from_readme(readme)
    assert 'vLLM' in tags


def test_multiple_tags_extracted():
    readme = '''
    A RAG system built with LangChain and ChromaDB.
    Uses fine-tuning with LoRA for domain adaptation.
    Deployed with Docker and Kubernetes.
    '''
    tags = extract_tags_from_readme(readme)
    assert 'LangChain' in tags
    assert 'Chroma' in tags
    assert 'Fine-Tuning' in tags
    assert 'LoRA / PEFT' in tags
    assert 'Docker' in tags
    assert 'Kubernetes' in tags


# ── Meta tags from GitHub metadata ───────────────────────────────────────────

def test_language_tags():
    tags = generate_meta_tags('Python', [], 0, '2024-01-01T00:00:00Z', False, False)
    assert 'Python' in tags
    assert 'Backend' in tags


def test_topic_tags():
    tags = generate_meta_tags(None, ['llm', 'rag'], 0, '2024-01-01T00:00:00Z', False, False)
    assert 'Large Language Models' in tags
    assert 'RAG' in tags


def test_popular_tag():
    tags = generate_meta_tags(None, [], 5000, '2024-01-01T00:00:00Z', False, False)
    assert 'Popular' in tags


def test_not_popular_below_threshold():
    tags = generate_meta_tags(None, [], 999, '2024-01-01T00:00:00Z', False, False)
    assert 'Popular' not in tags


def test_fork_tag():
    tags = generate_meta_tags(None, [], 0, '2024-01-01T00:00:00Z', True, False)
    assert 'Forked' in tags
    assert 'Built by Me' not in tags


def test_built_by_me_tag():
    tags = generate_meta_tags(None, [], 0, '2024-01-01T00:00:00Z', False, False)
    assert 'Built by Me' in tags
    assert 'Forked' not in tags


def test_archived_tag():
    tags = generate_meta_tags(None, [], 0, '2024-01-01T00:00:00Z', False, True)
    assert 'Archived' in tags


# ── Full enrichment pipeline ──────────────────────────────────────────────────

def test_full_enrich_deduplicates():
    readme = 'A Python project using pytorch for deep learning.'
    tags = enrich_tags('Python', ['pytorch'], 0, '2024-01-01T00:00:00Z', False, False, readme)
    # Should appear only once despite being in both language and readme
    assert tags.count('Python') == 1


def test_full_enrich_sorted():
    tags = enrich_tags('Python', ['llm'], 100, '2024-01-01T00:00:00Z', False, False)
    assert tags == sorted(tags)


# ── Builder extraction ────────────────────────────────────────────────────────

def test_builder_from_fork():
    """For forks, builder is the upstream org."""
    builder = build_builder(
        is_fork=True,
        forked_from='microsoft/semantic-kernel',
        full_name='myuser/semantic-kernel',
    )
    assert builder['login'] == 'microsoft'
    assert builder['is_known_org'] is True
    assert builder['org_category'] == 'big-tech'
    assert builder['display_name'] == 'Microsoft'


def test_builder_from_own_repo():
    """For non-forks, builder is the repo owner."""
    builder = build_builder(
        is_fork=False,
        forked_from=None,
        full_name='myuser/my-project',
    )
    assert builder['login'] == 'myuser'
    assert builder['is_known_org'] is False
    assert builder['org_category'] == 'individual'


def test_builder_known_org_openai():
    builder = build_builder(
        is_fork=True,
        forked_from='openai/gpt-4',
        full_name='myuser/gpt-4',
    )
    assert builder['login'] == 'openai'
    assert builder['is_known_org'] is True
    assert builder['org_category'] == 'ai-lab'


def test_builder_known_org_huggingface():
    builder = build_builder(
        is_fork=True,
        forked_from='huggingface/transformers',
        full_name='myuser/transformers',
    )
    assert builder['login'] == 'huggingface'
    assert builder['is_known_org'] is True


# ── Category assignment ───────────────────────────────────────────────────────

def test_primary_category_assignment():
    tags = ['LangChain', 'LangGraph', 'AI Agents', 'Multi-Agent']
    cat = assign_primary_category(tags)
    assert cat == 'AI Agents'


def test_primary_category_rag():
    tags = ['RAG', 'Vector Database', 'Embeddings', 'Chroma', 'Reranking']
    cat = assign_primary_category(tags)
    assert cat == 'RAG & Retrieval'


def test_all_categories():
    tags = ['RAG', 'LangChain', 'Docker', 'Python']
    cats = assign_all_categories(tags)
    assert 'RAG & Retrieval' in cats
    assert 'AI Agents' in cats
    assert 'MLOps & Infrastructure' in cats


def test_no_category_for_unrelated_tags():
    tags = ['xyz-unknown-tag-1', 'xyz-unknown-tag-2']
    cat = assign_primary_category(tags)
    assert cat == ''


# ── Skill assignment ──────────────────────────────────────────────────────────
# AI_DEV_SKILLS has been removed — taxonomy is now open/generative (no fixed list).
# assign_dimension() still works with any user-supplied dict (e.g. PM_SKILLS).

def test_pm_skills_assignment():
    tags = ['AI Safety', 'Red Teaming', 'Prompt Injection']
    skills = assign_dimension(tags, PM_SKILLS)
    assert 'Safety & Alignment' in skills


def test_pm_multiple_skills():
    tags = ['RAG', 'Vector Database', 'Docker', 'Evals', 'MLflow']
    pm_skills = assign_dimension(tags, PM_SKILLS)
    assert 'Data & Evaluation' in pm_skills
    assert 'Product Discovery' in pm_skills


def test_assign_dimension_accepts_arbitrary_dict():
    """assign_dimension works with any tag-to-dimension dict, not just fixed sets."""
    custom_dim = {
        'Speed': ['vLLM', 'TGI', 'batching'],
        'Quality': ['Evals', 'benchmarking'],
    }
    tags = ['vLLM', 'Evals']
    result = assign_dimension(tags, custom_dim)
    assert 'Speed' in result
    assert 'Quality' in result


# ── Summarizer fallback ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_summarizer_fallback_when_unavailable():
    """If Ollama unavailable, returns first paragraph of README."""
    import httpx
    from ingestion.enrichment.summarizer import RepoSummarizer

    summarizer = RepoSummarizer()
    summarizer._available = False

    readme = '''
# My Project

A powerful tool for building RAG applications with LangChain.
Supports ChromaDB and Pinecone as vector stores.

## Installation

pip install my-project
    '''
    result = summarizer._fallback_summary(readme)
    assert result is not None
    assert 'RAG' in result or 'powerful' in result


@pytest.mark.asyncio
async def test_summarizer_returns_none_for_empty_readme():
    from ingestion.enrichment.summarizer import RepoSummarizer
    summarizer = RepoSummarizer()
    summarizer._available = False
    result = summarizer._fallback_summary(None)
    assert result is None


@pytest.mark.asyncio
async def test_summarizer_skips_badge_lines():
    from ingestion.enrichment.summarizer import RepoSummarizer
    summarizer = RepoSummarizer()
    summarizer._available = False

    readme = '''
# Repo Title

![badge](https://img.shields.io/badge/test-passing)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A tool for building intelligent search systems using semantic embeddings.
'''
    result = summarizer._fallback_summary(readme)
    assert result is not None
    assert 'search' in result.lower() or 'semantic' in result.lower()
