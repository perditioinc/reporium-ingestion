"""#148: build the enrichment golden set used by the no-regression gate.

A golden record here is: the SAME enrichment prompt the production enricher
renders for a repo (so the candidate sees production-identical input), a
hand-curated REFERENCE taxonomy (the expected frontier-quality answer, used by
the NLI + judge tiers), and DETERMINISTIC taxonomy-shape checks (the ground-truth
Tier 1: valid JSON + every required field present + sane enum values + at least
one integration tag). The repos are a fixed, public, representative slice of the
AI/ML library corpus the ingestion pipeline enriches.

This emits ``eval/enrichment_goldenset.jsonl`` in the schema
``local_inference.eval.goldenset.load_goldenset`` consumes. Re-runnable and
deterministic - no network, no model calls.
"""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.enrichers.local_enricher import build_enrichment_prompt

HERE = Path(__file__).parent
OUT = HERE / "enrichment_goldenset.jsonl"

# Fixed, public AI/ML repos representative of the enrichment corpus. The
# `reference` is a curated frontier-quality taxonomy for that repo, used by the
# NLI/judge tiers; the deterministic checks below are appended programmatically
# so every record enforces the same taxonomy SHAPE contract.
REPOS = [
    {
        "context": {
            "owner": "langchain-ai", "name": "langchain",
            "description": "Build context-aware reasoning applications with LLMs: chains, agents, retrieval, memory.",
            "primary_language": "Python",
            "dependencies": ["pydantic", "requests", "sqlalchemy", "openai", "tiktoken"],
        },
        "reference": (
            "LangChain is a framework for building LLM-powered applications by composing chains, "
            "agents, retrieval and memory. It targets developers building RAG systems, agents and "
            "assistants. Skills: Retrieval-Augmented Generation, LLM Orchestration, Agentic AI. "
            "Industries: Developer Tools. Use cases: Document Question Answering, Conversational Agents. "
            "Modalities: Text. Trends: Agentic AI, Compound AI Systems. Deployment: Self-hosted, Cloud API. "
            "Integration tags: openai, pydantic, sqlalchemy, tiktoken."
        ),
        "expect_tags": ["openai"],
    },
    {
        "context": {
            "owner": "huggingface", "name": "transformers",
            "description": "State-of-the-art pretrained models for text, vision and audio: inference and fine-tuning.",
            "primary_language": "Python",
            "dependencies": ["torch", "tokenizers", "numpy", "safetensors"],
        },
        "reference": (
            "Transformers provides thousands of pretrained models for text, vision and audio with a "
            "unified API for inference and fine-tuning. Used by ML engineers and researchers. Skills: "
            "Transformer Architecture, Transfer Learning, Model Fine-tuning. Industries: AI Research. "
            "Use cases: Text Classification, Image Recognition, Speech Recognition. Modalities: Text, "
            "Image, Audio, Multimodal. Trends: Foundation Models, Multimodal Reasoning. Deployment: "
            "Self-hosted, Cloud API. Integration tags: pytorch, tokenizers, safetensors, numpy."
        ),
        "expect_tags": ["pytorch", "torch"],
    },
    {
        "context": {
            "owner": "vllm-project", "name": "vllm",
            "description": "High-throughput, memory-efficient inference and serving engine for LLMs with PagedAttention.",
            "primary_language": "Python",
            "dependencies": ["torch", "ray", "fastapi", "transformers"],
        },
        "reference": (
            "vLLM is a high-throughput, memory-efficient LLM inference and serving engine using "
            "PagedAttention. Used by platform teams deploying LLMs at scale. Skills: LLM Serving, "
            "GPU Optimization, Inference Acceleration. Industries: Developer Tools, AI Infrastructure. "
            "Use cases: Model Serving, Batch Inference. Modalities: Text. Trends: Efficient Inference, "
            "On-device AI. Deployment: Self-hosted, Cloud API, On-premise. Integration tags: pytorch, "
            "ray, fastapi, transformers."
        ),
        "expect_tags": ["fastapi"],
    },
    {
        "context": {
            "owner": "chroma-core", "name": "chroma",
            "description": "The open-source embedding database for building LLM apps with memory and retrieval.",
            "primary_language": "Python",
            "dependencies": ["numpy", "pydantic", "fastapi", "onnxruntime"],
        },
        "reference": (
            "Chroma is an open-source embedding (vector) database for storing and querying embeddings "
            "to give LLM applications retrieval and memory. Used by developers building RAG systems. "
            "Skills: Vector Search, Embeddings, Retrieval-Augmented Generation. Industries: Developer "
            "Tools. Use cases: Semantic Search, Document Retrieval. Modalities: Text. Trends: Compound "
            "AI Systems, RAG. Deployment: Self-hosted, Cloud API. Integration tags: pydantic, fastapi, "
            "numpy, onnxruntime."
        ),
        "expect_tags": ["fastapi"],
    },
    {
        "context": {
            "owner": "ggerganov", "name": "llama.cpp",
            "description": "LLM inference in C/C++ with minimal dependencies; runs quantized models on CPU and edge.",
            "primary_language": "C++",
            "dependencies": [],
        },
        "reference": (
            "llama.cpp runs LLM inference in portable C/C++ with minimal dependencies, enabling "
            "quantized models to run on CPUs and edge devices. Used by developers needing local, "
            "offline inference. Skills: Quantization, Edge Inference, Systems Programming. Industries: "
            "Developer Tools. Use cases: On-device Chat, Offline Inference. Modalities: Text. Trends: "
            "On-device AI, Small Language Models, Efficient Inference. Deployment: Edge/Mobile, "
            "Self-hosted, On-premise. Integration tags: ggml."
        ),
        "expect_tags": [],
    },
    {
        "context": {
            "owner": "openai", "name": "whisper",
            "description": "Robust speech recognition via large-scale weak supervision; transcription and translation.",
            "primary_language": "Python",
            "dependencies": ["torch", "numpy", "tiktoken", "ffmpeg-python"],
        },
        "reference": (
            "Whisper is a robust automatic speech recognition model trained with large-scale weak "
            "supervision, supporting multilingual transcription and translation. Used by developers "
            "adding voice features. Skills: Speech Recognition, Sequence-to-Sequence Modeling. "
            "Industries: Media, Accessibility. Use cases: Real-time Voice Transcription, Subtitling. "
            "Modalities: Audio, Text. Trends: Multimodal Reasoning, Foundation Models. Deployment: "
            "Self-hosted, Cloud API. Integration tags: pytorch, tiktoken, numpy, ffmpeg-python."
        ),
        "expect_tags": ["tiktoken"],
    },
    {
        "context": {
            "owner": "ultralytics", "name": "ultralytics",
            "description": "YOLO models for real-time object detection, segmentation, pose estimation and classification.",
            "primary_language": "Python",
            "dependencies": ["torch", "opencv-python", "numpy", "pillow"],
        },
        "reference": (
            "Ultralytics provides YOLO models for real-time computer vision: object detection, "
            "segmentation, pose estimation and classification. Used by ML engineers building vision "
            "systems. Skills: Object Detection, Computer Vision, Model Training. Industries: Robotics, "
            "Manufacturing, Security. Use cases: Real-time Detection, Image Segmentation. Modalities: "
            "Image, Video. Trends: Edge AI, Real-time Vision. Deployment: Edge/Mobile, Self-hosted. "
            "Integration tags: pytorch, opencv, pillow, numpy."
        ),
        "expect_tags": ["pytorch", "opencv"],
    },
    {
        "context": {
            "owner": "ray-project", "name": "ray",
            "description": "A unified framework for scaling AI and Python workloads: distributed training, serving and tuning.",
            "primary_language": "Python",
            "dependencies": ["grpcio", "protobuf", "numpy", "pydantic"],
        },
        "reference": (
            "Ray is a unified framework for scaling Python and AI workloads, with libraries for "
            "distributed training, hyperparameter tuning and model serving. Used by ML platform teams. "
            "Skills: Distributed Computing, Model Training, Hyperparameter Tuning. Industries: AI "
            "Infrastructure, Developer Tools. Use cases: Distributed Training, Model Serving. "
            "Modalities: Text, Tabular. Trends: Compound AI Systems, MLOps. Deployment: Self-hosted, "
            "Cloud API, On-premise. Integration tags: grpcio, protobuf, pydantic, numpy."
        ),
        "expect_tags": [],
    },
]

# The deterministic taxonomy-SHAPE contract every enrichment output must satisfy.
# These are the ground truth the gate's Tier 1 enforces (model-free).
QUALITY_ENUM = "(high|medium|low)"
MATURITY_ENUM = "(research|prototype|beta|production)"

_REQUIRED_KEYS = [
    "readme_summary", "problem_solved", "quality_assessment", "maturity_level",
    "skill_areas", "industries", "use_cases", "modalities", "ai_trends",
    "deployment_context", "integration_tags",
]


def _shape_checks(expect_nonempty_tags: bool) -> list[dict]:
    """The production taxonomy SHAPE contract (the thing the KAN-191 incident was
    about): valid JSON, every required field present, in-vocab enum values, and -
    for repos with real dependencies - a non-empty integration_tags array.

    Note we deliberately do NOT assert SPECIFIC tag strings. The production
    parser accepts whatever the model emits (lowercased + deduped); requiring a
    literal dependency name back would over-fit the gate to one model's phrasing
    and is not part of the real contract. The structural 'integration_tags is a
    non-empty list' check captures the actual regression risk."""
    checks: list[dict] = [{"kind": "json_valid"}]
    for key in _REQUIRED_KEYS:
        checks.append({"kind": "json_key", "key": key})
    # Enum sanity: the quality + maturity values must be in-vocabulary.
    checks.append({"kind": "regex", "pattern": rf'"quality_assessment"\s*:\s*"{QUALITY_ENUM}"'})
    checks.append({"kind": "regex", "pattern": rf'"maturity_level"\s*:\s*"{MATURITY_ENUM}"'})
    if expect_nonempty_tags:
        # integration_tags must contain at least one non-empty string element.
        # Matches "integration_tags": ["something" (any first element present).
        checks.append(
            {"kind": "regex", "pattern": r'"integration_tags"\s*:\s*\[\s*"[^"]+"'}
        )
    return checks


def main() -> None:
    records = []
    for i, repo in enumerate(REPOS):
        ctx = repo["context"]
        # Repos with real dependencies must yield a non-empty integration_tags
        # array; dependency-free repos (e.g. llama.cpp, ray's curated set) may
        # legitimately have an empty array, so we relax that one check for them.
        expect_nonempty = bool(ctx.get("dependencies"))
        records.append(
            {
                "id": f"{ctx['owner']}/{ctx['name']}",
                "input": build_enrichment_prompt(ctx),
                "reference": repo["reference"],
                "checks": _shape_checks(expect_nonempty),
            }
        )
    with OUT.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(records)} golden records -> {OUT}")


if __name__ == "__main__":
    main()
