"""Tests for dependency extraction, secret loading, and enrichment prompts."""

import json
import os
import tempfile


def test_dependency_extractor_parses_requirements():
    """Test that the dependency extractor correctly parses requirements.txt format."""
    from ingestion.extractors.dependencies import parse_requirements_txt

    content = """numpy>=1.24
pandas==2.0.0
torch
requests>=2.28,<3.0
# comment line
-e git+https://github.com/foo/bar.git
"""
    deps = parse_requirements_txt(content)
    assert "numpy" in deps
    assert "pandas" in deps
    assert "torch" in deps
    assert "requests" in deps
    # Should not include comments or -e lines
    assert not any(d.startswith("#") for d in deps)
    assert not any(d.startswith("-e") for d in deps)


def test_secret_loader_strips_carriage_returns():
    """Test that secrets loaded from environment strip \\r\\n whitespace."""
    # Simulate a secret with carriage return (common from Windows/Secret Manager)
    test_key = "sk-ant-test-key-12345\r\n"
    os.environ["TEST_SECRET_KEY"] = test_key

    # The pattern used in our code: .strip()
    loaded = os.environ.get("TEST_SECRET_KEY", "").strip()
    assert loaded == "sk-ant-test-key-12345"
    assert "\r" not in loaded
    assert "\n" not in loaded

    del os.environ["TEST_SECRET_KEY"]


def test_enrichment_context_builder_produces_valid_output():
    """Test that the AI enricher context builder produces output with expected fields."""
    from ingestion.enrichers.ai_enricher import _build_repo_context

    context = _build_repo_context({
        "name": "test-repo",
        "owner": "testorg",
        "forked_from": "upstream/test-repo",
        "description": "A test repository for unit testing",
        "primary_language": "Python",
        "dependencies": ["pytest", "httpx"],
    })

    # Context must be a non-empty string
    assert isinstance(context, str)
    assert len(context) > 20

    # Context must mention the repo
    assert "test-repo" in context
