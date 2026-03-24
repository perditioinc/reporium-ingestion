"""
Category, skill, and builder taxonomy — ported from reporium TypeScript source.
"""

# ── 21 hardcoded categories ───────────────────────────────────────────────────

CATEGORIES: list[dict] = [
    {
        'id': 'foundation-models',
        'name': 'Foundation Models',
        'tags': [
            'Large Language Models', 'Transformers', 'OpenAI', 'Anthropic / Claude',
            'Google AI', 'HuggingFace', 'Long Context', 'Multimodal AI',
            'Quantization', 'llama.cpp', 'GGUF'
        ],
    },
    {
        'id': 'ai-agents',
        'name': 'AI Agents',
        'tags': [
            'AI Agents', 'Multi-Agent', 'Autonomous Systems', 'Agent Memory',
            'Planning / CoT', 'Tool Use', 'LangChain', 'LangGraph', 'CrewAI',
            'AutoGen', 'MCP', 'Prompt Engineering', 'Context Engineering',
            'Structured Output', 'Function Calling'
        ],
    },
    {
        'id': 'rag-retrieval',
        'name': 'RAG & Retrieval',
        'tags': [
            'RAG', 'Vector Database', 'Embeddings', 'Knowledge Graph',
            'Semantic Search', 'Hybrid Search', 'Reranking', 'LlamaIndex',
            'Document Processing', 'Chunking'
        ],
    },
    {
        'id': 'model-training',
        'name': 'Model Training',
        'tags': [
            'Fine-Tuning', 'Reinforcement Learning', 'LoRA / PEFT', 'RLHF',
            'Synthetic Data', 'Dataset', 'Training Infrastructure',
            'Unsloth', 'Axolotl', 'TRL', 'DeepSpeed', 'FSDP',
            'PyTorch', 'TensorFlow', 'Keras', 'JAX'
        ],
    },
    {
        'id': 'evals-benchmarking',
        'name': 'Evals & Benchmarking',
        'tags': [
            'Evals', 'Benchmarking', 'Model Evaluation', 'LLM Testing',
            'Red Teaming', 'Safety Evaluation', 'MMLU', 'HumanEval',
            'Code Evaluation', 'Alignment'
        ],
    },
    {
        'id': 'observability',
        'name': 'Observability & Monitoring',
        'tags': [
            'Observability', 'Tracing', 'Monitoring', 'LLM Monitoring',
            'Logging', 'Debugging', 'LangSmith', 'Phoenix', 'MLflow',
            'Weights & Biases', 'Experiment Tracking'
        ],
    },
    {
        'id': 'inference-serving',
        'name': 'Inference & Serving',
        'tags': [
            'Inference', 'LLM Serving', 'Model Optimization', 'vLLM',
            'TensorRT', 'Triton', 'Ollama', 'TGI', 'Batching',
            'Caching', 'GPU / CUDA', 'Real-Time / Streaming'
        ],
    },
    {
        'id': 'generative-media',
        'name': 'Generative Media',
        'tags': [
            'Image Generation', 'Video Generation', 'Text to Speech',
            'Speech to Text', 'Music / Audio AI', 'ComfyUI',
            'Diffusion Models', 'ControlNet', 'LoRA', 'Stable Diffusion'
        ],
    },
    {
        'id': 'computer-vision',
        'name': 'Computer Vision',
        'tags': [
            'Computer Vision', 'Point Cloud / 3D Vision', 'Object Detection',
            'Segmentation', 'Depth Estimation', 'SLAM',
            'Optical Flow', '3D Reconstruction', 'Pose Estimation'
        ],
    },
    {
        'id': 'robotics',
        'name': 'Robotics',
        'tags': [
            'Robotics', 'Robot Arms', 'Robot Learning', 'Humanoid Robotics',
            'Simulation', 'ROS', 'Motion Planning', 'Grasping',
            'Manipulation', 'Navigation', 'Control Systems'
        ],
    },
    {
        'id': 'spatial-xr',
        'name': 'Spatial & XR',
        'tags': [
            'XR / Spatial Computing', 'Virtual Reality', 'Augmented Reality',
            'Immersive Media', 'WebXR', 'Spatial AI', 'ARKit', 'ARCore',
            'Meta Quest', 'Apple Vision'
        ],
    },
    {
        'id': 'mlops-infrastructure',
        'name': 'MLOps & Infrastructure',
        'tags': [
            'MLOps', 'Docker', 'Kubernetes', 'CI/CD', 'Pipeline',
            'Feature Store', 'Model Registry', 'Data Versioning',
            'DVC', 'ZenML', 'Prefect', 'Airflow', 'Ray',
            'Distributed Computing', 'DevOps'
        ],
    },
    {
        'id': 'dev-tools',
        'name': 'Dev Tools & Automation',
        'tags': [
            'CLI Tool', 'API', 'Automation', 'SDK', 'Developer Tools',
            'Code Generation', 'Coding Assistant', 'Systems', 'Security',
            'Database', 'Backend', 'Frontend', 'Full Stack', 'Node.js',
            'React / Next.js', 'Python Web Framework', 'Web3'
        ],
    },
    {
        'id': 'cloud-platforms',
        'name': 'Cloud & Platforms',
        'tags': [
            'Google Cloud', 'AWS', 'Azure', 'Google AI',
            'Vertex AI', 'SageMaker', 'Bedrock'
        ],
    },
    {
        'id': 'learning-resources',
        'name': 'Learning Resources',
        'tags': [
            'Tutorial', 'Course', 'Roadmap', 'Cheat Sheet', 'Curated List',
            'Interview Prep', 'Research / Papers', 'Open Source', 'Book',
            'Workshop', 'Lecture Notes'
        ],
    },
    {
        'id': 'industry-healthcare',
        'name': 'Industry: Healthcare',
        'tags': [
            'Healthcare AI', 'Medical Imaging', 'Drug Discovery',
            'Clinical NLP', 'Bioinformatics', 'Genomics'
        ],
    },
    {
        'id': 'industry-fintech',
        'name': 'Industry: FinTech',
        'tags': [
            'FinTech', 'Trading AI', 'Risk Modeling',
            'Fraud Detection', 'Financial NLP'
        ],
    },
    {
        'id': 'industry-audio-music',
        'name': 'Industry: Audio & Music',
        'tags': [
            'Music Tech', 'Audio AI', 'Music / Audio AI',
            'Music Generation', 'Audio Processing', 'Voice Cloning'
        ],
    },
    {
        'id': 'industry-gaming',
        'name': 'Industry: Gaming',
        'tags': [
            'Game Dev', 'NPC AI', 'Procedural Generation',
            'Game AI', 'Simulation'
        ],
    },
    {
        'id': 'security-safety',
        'name': 'Security & Safety',
        'tags': [
            'Security', 'AI Safety', 'Red Teaming', 'Alignment',
            'Adversarial', 'Privacy', 'Watermarking'
        ],
    },
    {
        'id': 'data-science',
        'name': 'Data Science & Analytics',
        'tags': [
            'Data Science', 'Analytics', 'Visualization', 'Statistics',
            'Pandas', 'NumPy', 'Jupyter', 'Data Engineering'
        ],
    },
]

# ── Lifecycle groups (reference documentation only — no longer used for validation) ──

LIFECYCLE_GROUPS: list[str] = [
    "Foundation & Training",
    "Inference & Deployment",
    "LLM Application Layer",
    "Eval/Safety/Ops",
    "Modality-Specific",
    "Applied AI",
]

# AI_DEV_SKILLS and SKILL_TO_LIFECYCLE_GROUP have been removed.
# Taxonomy skill areas are now generated freely by the AI enricher (open taxonomy).
# The assign_dimension() helper below still works against any dict you provide.

# ── PM Skills ─────────────────────────────────────────────────────────────────

PM_SKILLS: dict[str, list[str]] = {
    'Cost & Efficiency': [
        'LiteLLM', 'Quantization', 'LLM Serving', 'KV Cache',
        'Speculative Decoding', 'Caching', 'vLLM', 'Inference'
    ],
    'Safety & Alignment': [
        'AI Safety', 'Red Teaming', 'Guardrails', 'Garak', 'PyRIT',
        'Prompt Injection', 'Alignment', 'Privacy-Preserving AI'
    ],
    'User Experience': [
        'Text to Speech', 'Speech to Text', 'Multimodal AI',
        'Frontend', 'React / Next.js', 'Voice Cloning', 'WebXR'
    ],
    'Scale & Reliability': [
        'MLOps', 'Docker', 'Kubernetes', 'Ray', 'Monitoring',
        'Tracing', 'LLM Monitoring', 'Real-Time / Streaming'
    ],
    'Data & Evaluation': [
        'Evals', 'DeepEval', 'RAGAS', 'Benchmarking', 'Synthetic Data',
        'Data Science', 'Dataset', 'MLflow'
    ],
    'Product Discovery': [
        'RAG', 'Embeddings', 'Vector Database', 'Semantic Search',
        'Knowledge Graph', 'Reranking', 'Document Processing'
    ],
    'Developer Platform': [
        'API', 'SDK', 'CLI Tool', 'MCP', 'Tool Use', 'Automation',
        'Structured Output', 'Webhook'
    ],
    'AI-Native Architecture': [
        'AI Agents', 'Multi-Agent', 'Agent Memory', 'Context Engineering',
        'Planning / CoT', 'LangGraph', 'Autonomous Systems'
    ],
}

# ── Known organizations ───────────────────────────────────────────────────────

KNOWN_ORGS: dict[str, dict] = {
    'google': {'category': 'big-tech', 'display_name': 'Google'},
    'google-deepmind': {'category': 'ai-lab', 'display_name': 'Google DeepMind'},
    'google-gemini': {'category': 'big-tech', 'display_name': 'Google Gemini'},
    'microsoft': {'category': 'big-tech', 'display_name': 'Microsoft'},
    'meta-llama': {'category': 'big-tech', 'display_name': 'Meta'},
    'facebookresearch': {'category': 'ai-lab', 'display_name': 'Meta Research'},
    'openai': {'category': 'ai-lab', 'display_name': 'OpenAI'},
    'anthropics': {'category': 'ai-lab', 'display_name': 'Anthropic'},
    'huggingface': {'category': 'ai-lab', 'display_name': 'HuggingFace'},
    'mistralai': {'category': 'ai-lab', 'display_name': 'Mistral AI'},
    'deepseek-ai': {'category': 'ai-lab', 'display_name': 'DeepSeek'},
    'qwenlm': {'category': 'ai-lab', 'display_name': 'Qwen / Alibaba'},
    'nvidia': {'category': 'big-tech', 'display_name': 'NVIDIA'},
    'aws': {'category': 'big-tech', 'display_name': 'Amazon AWS'},
    'apple': {'category': 'big-tech', 'display_name': 'Apple'},
    'langchain-ai': {'category': 'startup', 'display_name': 'LangChain'},
    'vllm-project': {'category': 'startup', 'display_name': 'vLLM'},
    'unslothai': {'category': 'startup', 'display_name': 'Unsloth'},
    'langfuse': {'category': 'startup', 'display_name': 'Langfuse'},
    'chroma-core': {'category': 'startup', 'display_name': 'Chroma'},
    'qdrant': {'category': 'startup', 'display_name': 'Qdrant'},
    'weaviate': {'category': 'startup', 'display_name': 'Weaviate'},
    'infiniflow': {'category': 'startup', 'display_name': 'Infiniflow'},
    'arize-ai': {'category': 'startup', 'display_name': 'Arize AI'},
    'confident-ai': {'category': 'startup', 'display_name': 'Confident AI'},
    'run-llama': {'category': 'startup', 'display_name': 'LlamaIndex'},
    'letta-ai': {'category': 'startup', 'display_name': 'Letta'},
    'mem0ai': {'category': 'startup', 'display_name': 'Mem0'},
    'crewaiinc': {'category': 'startup', 'display_name': 'CrewAI'},
    'agno-agi': {'category': 'startup', 'display_name': 'Agno'},
    'all-hands-ai': {'category': 'startup', 'display_name': 'All Hands AI'},
    'cline': {'category': 'startup', 'display_name': 'Cline'},
    'continuedev': {'category': 'startup', 'display_name': 'Continue'},
    'browser-use': {'category': 'startup', 'display_name': 'Browser Use'},
    'eleutherai': {'category': 'ai-lab', 'display_name': 'EleutherAI'},
    'allenai': {'category': 'ai-lab', 'display_name': 'Allen AI'},
    'stanford-crfm': {'category': 'research', 'display_name': 'Stanford'},
    'mit-han-lab': {'category': 'research', 'display_name': 'MIT Han Lab'},
}


def assign_primary_category(tags: list[str]) -> str:
    """Return category name with most matching tags. Empty string if none."""
    best_name = ''
    best_count = 0
    for cat in CATEGORIES:
        count = sum(1 for t in tags if t in cat['tags'])
        if count > best_count:
            best_count = count
            best_name = cat['name']
    return best_name


def assign_all_categories(tags: list[str]) -> list[str]:
    """Return all category names that have at least one matching tag."""
    return [cat['name'] for cat in CATEGORIES if any(t in cat['tags'] for t in tags)]


def assign_dimension(tags: list[str], dimension_map: dict[str, list[str]]) -> list[str]:
    """Return all dimension keys where at least one tag matches."""
    return [dim for dim, dim_tags in dimension_map.items() if any(t in dim_tags for t in tags)]


def build_builder(is_fork: bool, forked_from: str | None, full_name: str) -> dict:
    """Derive builder metadata from repo ownership."""
    if is_fork and forked_from:
        original_owner = forked_from.split('/')[0]
    else:
        original_owner = full_name.split('/')[0]

    key = original_owner.lower()
    known = KNOWN_ORGS.get(key)
    return {
        'login': original_owner,
        'display_name': known['display_name'] if known else original_owner,
        'is_known_org': bool(known),
        'org_category': known['category'] if known else 'individual',
    }
