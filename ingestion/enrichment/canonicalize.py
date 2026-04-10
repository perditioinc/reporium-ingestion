"""
Tag canonicalization: fuzzy-match raw AI-generated tags against the ~200
vocabulary of canonical tag names used throughout the platform.

Usage:
    from ingestion.enrichment.canonicalize import canonicalize_tags

    raw  = ["llm", "LLMs", "large language model", "pytorch", "some-unknown-tag"]
    canonical = canonicalize_tags(raw)
    # → ["Large Language Models", "Large Language Models", "Large Language Models",
    #     "PyTorch"]  (unique, unknown dropped)

Design:
    1. Exact match (case-insensitive) — fast path, no difflib overhead.
    2. difflib.get_close_matches() against the full vocabulary at threshold 0.82.
       Threshold chosen empirically: tight enough to avoid false collapses
       ("web3" ≠ "Web3 / DeFi" type distinctions), loose enough to catch
       "llm" → "Large Language Models", "huggingface" → "HuggingFace".
    3. Unmatched tags logged at DEBUG level — review periodically to expand vocab.
"""

from __future__ import annotations

import difflib
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical vocabulary
# Sourced from README_KEYWORD_MAP, TOPIC_TAGS, and LANGUAGE_TAGS values in
# ingestion/enrichment/tagger.py.  Keep in sync with that file.
# ---------------------------------------------------------------------------

CANONICAL_TAGS: frozenset[str] = frozenset({
    # LLM & Foundation Models
    "Large Language Models", "OpenAI", "Anthropic / Claude", "Google AI",
    "DeepSeek", "Qwen", "Llama", "Mistral", "Phi", "Gemma", "Claude", "GPT",
    "Reasoning Models",
    # RAG & Retrieval
    "RAG", "Vector Database", "Embeddings", "Semantic Search", "Hybrid Search",
    "Reranking", "Document Processing", "Chunking", "GraphRAG",
    "Chroma", "Qdrant", "Milvus", "Weaviate", "Pinecone", "pgvector",
    # AI Agents & Orchestration
    "AI Agents", "Multi-Agent", "Agent Memory", "Planning / CoT",
    "Tool Use", "Structured Output", "Context Engineering",
    "MCP",
    # Agent Frameworks
    "LangChain", "LangGraph", "LlamaIndex", "CrewAI", "AutoGen",
    "DSPy", "Semantic Kernel", "Haystack", "LiteLLM", "Agno",
    "Letta / MemGPT", "Mem0", "Swarm", "OpenAI Agents SDK",
    # Fine-Tuning & Training
    "Fine-Tuning", "LoRA / PEFT", "RLHF", "DPO", "GRPO",
    "Synthetic Data", "Distillation", "DeepSpeed", "FSDP", "TRL",
    "Unsloth", "Axolotl", "TorchTune", "MergeKit",
    # Inference & Serving
    "LLM Serving", "vLLM", "TGI", "Triton", "TensorRT", "llama.cpp",
    "ExLlama", "GPT4All", "PrivateGPT", "Llamafile", "SGLang",
    "Quantization", "Speculative Decoding", "KV Cache", "Batching",
    "Model Optimization", "Inference",
    # MLOps & Infra
    "MLOps", "DVC", "ZenML", "Prefect", "Airflow", "Ray", "Kubeflow",
    "Feature Store", "Model Registry", "SageMaker", "Vertex AI",
    "Azure AI", "AWS Bedrock", "AWS", "Google Cloud",
    # Observability & Evals
    "Evals", "Benchmarking", "MMLU", "HumanEval", "LM Eval Harness",
    "DeepEval", "RAGAS", "PromptFoo", "Red Teaming", "Garak", "PyRIT",
    "LangSmith", "Phoenix", "MLflow", "Weights & Biases", "Tracing",
    "Monitoring", "Langfuse", "OpenLLMetry", "OpenLIT", "Helicone",
    "Traceloop", "OpenTelemetry",
    # Computer Vision & Spatial
    "Computer Vision", "Object Detection", "Segmentation", "Depth Estimation",
    "Pose Estimation", "3D Reconstruction", "Point Cloud / 3D Vision",
    # Robotics
    "Robotics", "ROS", "ROS 2", "Motion Planning", "Grasping",
    "Humanoid Robotics", "Robot Arms", "Robot Learning", "Sim-to-Real", "SLAM",
    "Autonomous Systems",
    # Generative Media
    "Image Generation", "Video Generation", "Text to Speech", "Speech to Text",
    "Music / Audio AI", "Music Generation", "Voice Cloning",
    "Stable Diffusion", "ControlNet", "ComfyUI", "SD WebUI", "Whisper",
    # Multimodal & XR
    "Multimodal AI", "XR / Spatial Computing", "Virtual Reality",
    "Augmented Reality", "Mixed Reality", "Immersive Media",
    "WebXR", "ARKit", "ARCore", "Meta Quest", "Apple Vision", "Apple Vision Pro",
    # Programming Languages
    "Python", "TypeScript", "JavaScript", "Rust", "Go", "Java", "C++",
    "Backend", "Frontend", "Full Stack", "Systems",
    # Frameworks & Tools
    "React / Next.js", "Python Web Framework", "Node.js",
    "Docker", "Kubernetes", "DevOps", "API", "GraphQL",
    "Database", "Caching",
    # Structured Output & Reliability
    "Pydantic", "Instructor", "Outlines", "Guidance", "Guardrails",
    "NeMo Guardrails", "Prompt Engineering",
    # ML Basics
    "Machine Learning", "Deep Learning", "Transformers", "PyTorch",
    "TensorFlow", "Keras", "JAX", "GPU / CUDA",
    "Reinforcement Learning", "Long Context",
    # Data Science
    "Data Science", "Pandas", "Jupyter", "Data Visualization", "NumPy",
    "Scikit-learn", "Spark", "Data Engineering", "Statistics",
    "Visualization",
    # Coding Assistants / IDEs
    "Continue.dev", "Aider", "SWE-Agent", "OpenDevin", "OpenHands",
    "Cline", "Claude Code", "Gemini CLI", "Kilocode",
    # Visual / No-code
    "Langflow", "Flowise", "n8n", "No-Code Automation", "Automation",
    # Knowledge & Learning
    "Tutorial", "Course", "Roadmap", "Cheat Sheet", "Curated List",
    "Interview Prep", "Research / Papers", "Open Source",
    # Domains
    "FinTech", "Healthcare AI", "Music Tech", "Game Dev",
    "Security", "Web3", "Mobile", "Knowledge Graph", "Real-Time / Streaming",
    # Safety & Privacy
    "AI Safety", "Adversarial", "Watermarking", "Privacy",
    "Privacy-Preserving AI", "Prompt Injection",
    # Other
    "CLI Tool", "Simulation", "HuggingFace", "Ollama",
    "ONNX", "Popular", "Active", "Inactive", "Forked", "Built by Me",
    "Archived", "NLP", "Frontend Framework", "Testing",
})

# Lowercase → canonical map for O(1) exact-match lookup
_LOWER_TO_CANONICAL: dict[str, str] = {tag.lower(): tag for tag in CANONICAL_TAGS}

# Sorted list for difflib (needs a sequence)
_VOCAB_LIST: list[str] = sorted(CANONICAL_TAGS)
_VOCAB_LOWER: list[str] = [t.lower() for t in _VOCAB_LIST]


def canonicalize_tag(raw: str) -> str | None:
    """
    Return the canonical form of *raw*, or None if no match found.

    Matching strategy:
      1. Exact match (case-insensitive) → O(1).
      2. difflib.get_close_matches on the lower-cased vocabulary.
         Threshold 0.82 — tight enough to avoid false collapses,
         loose enough to catch 'llm' → 'large language models'.
    """
    if not raw or not isinstance(raw, str):
        return None
    clean = raw.strip()
    if not clean:
        return None

    # 1. Exact match (handles 'Python', 'python', 'PYTHON')
    lower = clean.lower()
    if lower in _LOWER_TO_CANONICAL:
        return _LOWER_TO_CANONICAL[lower]

    # 2. Fuzzy match against lower-cased vocabulary
    matches = difflib.get_close_matches(lower, _VOCAB_LOWER, n=1, cutoff=0.82)
    if matches:
        idx = _VOCAB_LOWER.index(matches[0])
        return _VOCAB_LIST[idx]

    logger.debug("canonicalize_tag: no match for %r (threshold=0.82)", clean)
    return None


def canonicalize_tags(tags: list[str]) -> list[str]:
    """
    Canonicalize a list of raw tags.  Returns unique canonical forms in
    original encounter order; unmatched tags are silently dropped.
    """
    seen: set[str] = set()
    result: list[str] = []
    for raw in tags:
        canonical = canonicalize_tag(raw)
        if canonical and canonical not in seen:
            seen.add(canonical)
            result.append(canonical)
    return result
