# Enrichment Prompt V2

## Canonical Source

The canonical category list lives in code:
**`ingestion/enrichment/taxonomy.py`** (`CATEGORIES`).

The DB column `repos.primary_category` stores the **NAME** (e.g., `"AI Agents"`),
not an ID. Treat names as the canonical identifier. The `id` field on each entry
in `taxonomy.py` exists only as a slug for legacy code paths and is not used by
the enrichment write path or by `repos.primary_category`.

If you find this doc disagrees with `taxonomy.py`, taxonomy.py wins — update
this doc, do not fork the vocabulary here.

## 21-Category Fixed Taxonomy

Every repo MUST be assigned exactly ONE `primary_category` from this list
(use the NAME, exactly as written, including punctuation and casing):

1. **Foundation Models** — base LLMs, transformers, multimodal, quantization (llama.cpp, GGUF), long context.
2. **AI Agents** — autonomous systems, multi-agent, agent memory, planning/CoT, tool use (LangChain, LangGraph, CrewAI, AutoGen, MCP), prompt/context engineering, structured output, function calling.
3. **RAG & Retrieval** — vector DBs, embeddings, knowledge graphs, semantic/hybrid search, reranking, LlamaIndex, document processing, chunking.
4. **Model Training** — fine-tuning, RL/RLHF, LoRA / PEFT, synthetic data, datasets, training infra (Unsloth, Axolotl, TRL, DeepSpeed, FSDP), PyTorch / TensorFlow / Keras / JAX.
5. **Evals & Benchmarking** — evals, model evaluation, LLM testing, red teaming, safety evaluation, MMLU, HumanEval, code evaluation, alignment.
6. **Observability & Monitoring** — tracing, monitoring, LLM monitoring, logging, debugging, LangSmith, Phoenix, MLflow, Weights & Biases, experiment tracking.
7. **Inference & Serving** — vLLM, TensorRT, Triton, Ollama, TGI, batching, caching, GPU/CUDA, real-time / streaming, model optimization.
8. **Generative Media** — image/video generation, TTS, STT, music/audio AI, ComfyUI, diffusion, ControlNet, LoRA, Stable Diffusion.
9. **Computer Vision** — point cloud / 3D vision, object detection, segmentation, depth estimation, SLAM, optical flow, 3D reconstruction, pose estimation.
10. **Robotics** — robot arms, robot learning, humanoid robotics, simulation, ROS, motion planning, grasping, manipulation, navigation, control systems.
11. **Spatial & XR** — XR / spatial computing, VR, AR, immersive media, WebXR, spatial AI, ARKit, ARCore, Meta Quest, Apple Vision.
12. **MLOps & Infrastructure** — Docker, Kubernetes, CI/CD, pipelines, feature store, model registry, data versioning, DVC, ZenML, Prefect, Airflow, Ray, distributed computing, DevOps.
13. **Dev Tools & Automation** — CLI tools, APIs, automation, SDKs, developer tools, code generation, coding assistants, systems, security, database, backend, frontend, full stack, Node.js, React/Next.js, Python web frameworks, Web3.
14. **Cloud & Platforms** — Google Cloud, AWS, Azure, Vertex AI, SageMaker, Bedrock.
15. **Learning Resources** — tutorials, courses, roadmaps, cheat sheets, curated lists, interview prep, research/papers, open source, books, workshops, lecture notes.
16. **Industry: Healthcare** — healthcare AI, medical imaging, drug discovery, clinical NLP, bioinformatics, genomics.
17. **Industry: FinTech** — fintech, trading AI, risk modeling, fraud detection, financial NLP.
18. **Industry: Audio & Music** — music tech, audio AI, music generation, audio processing, voice cloning.
19. **Industry: Gaming** — game dev, NPC AI, procedural generation, game AI, simulation.
20. **Security & Safety** — AI safety, red teaming, alignment, adversarial, privacy, watermarking.
21. **Data Science & Analytics** — analytics, visualization, statistics, Pandas, NumPy, Jupyter, data engineering.

> Programmatic check:
> ```bash
> python -c "from ingestion.enrichment.taxonomy import CATEGORIES; print(len(CATEGORIES))"
> # -> 21
> ```

## Prompt Template
Given a GitHub repository with this context:
- Name: {name}
- Description: {description}
- README (first 2000 chars): {readme}
- Languages: {languages}
- Topics: {topics}
- Dependencies: {dependencies}

Respond with valid JSON only:
```json
{
  "primary_category": "<one of the 21 category NAMES, e.g. \"AI Agents\">",
  "secondary_categories": ["<up to 3 additional category NAMES>"],
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
```

## Validation Rules
- `primary_category` MUST be one of the 21 NAMES above (reject if not).
- `secondary_categories` max 3, all from the same 21-name list.
- `quality_signals` is required.
- `ai_dev_skills` maps to the coverage badges on the dashboard.

## Additive Pattern
- NEVER delete existing enrichment data before new data is verified.
- Write to NEW columns or UPDATE only NULL fields first.
- Verify counts match expectations before bulk UPDATE of existing data.
- If enrichment fails midway, already-enriched repos keep their data.

## Drift caveats (for operators)

The aggregates surface (`/library/aggregates -> categories`) may show **more
than 21** category names. Extra entries are pre-21-taxonomy strings still
sitting in `repos.primary_category` from earlier vocabulary generations
(examples observed in prod include `Coding & Dev Tools`, `Edge & Mobile AI`,
`Other AI / ML`, `Multimodal AI`, `Search & Knowledge`, `Safety & Alignment`,
`Healthcare & Biology`, `Finance & Legal`, `ML Platform & Infrastructure`,
`NLP & Text`). These are **not** in the canonical taxonomy and should be
remapped or cleaned in a separate data-hygiene ticket — they are not new
vocabulary. New ingest runs only emit the 21 names above (per
`assign_primary_category`).
