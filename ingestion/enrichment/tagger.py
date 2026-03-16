"""
Keyword-based tag extraction ported from reporium/src/lib/enrichRepo.ts.
Exact parity with the TypeScript implementation.
"""
import re
from datetime import datetime, timezone

# ── Language → tags ───────────────────────────────────────────────────────────

LANGUAGE_TAGS: dict[str, list[str]] = {
    'Python': ['Python', 'Backend'],
    'TypeScript': ['TypeScript', 'Frontend'],
    'JavaScript': ['JavaScript', 'Full Stack'],
    'Rust': ['Rust', 'Systems'],
    'Go': ['Go', 'Backend'],
    'Java': ['Java', 'Backend'],
    'C++': ['C++', 'Systems'],
    'C': ['C', 'Systems'],
    'C#': ['C#', 'Backend'],
    'Shell': ['Shell', 'DevOps'],
    'Bash': ['Bash', 'DevOps'],
    'Ruby': ['Ruby', 'Backend'],
    'PHP': ['PHP', 'Backend'],
    'Swift': ['Swift', 'Mobile'],
    'Kotlin': ['Kotlin', 'Mobile'],
    'Dart': ['Dart', 'Mobile'],
    'Scala': ['Scala', 'Backend'],
    'Elixir': ['Elixir', 'Backend'],
    'Haskell': ['Haskell', 'Systems'],
    'R': ['R', 'Data Science'],
}

# ── Topic → tag ───────────────────────────────────────────────────────────────

TOPIC_TAGS: dict[str, str] = {
    'llm': 'Large Language Models',
    'gpt': 'Large Language Models',
    'openai': 'Large Language Models',
    'anthropic': 'Large Language Models',
    'claude': 'Large Language Models',
    'chatgpt': 'Large Language Models',
    'rag': 'RAG',
    'retrieval': 'RAG',
    'vector-db': 'RAG',
    'vectordb': 'RAG',
    'pinecone': 'RAG',
    'agent': 'AI Agents',
    'agents': 'AI Agents',
    'agentic': 'AI Agents',
    'langgraph': 'AI Agents',
    'autogen': 'AI Agents',
    'crewai': 'AI Agents',
    'computer-vision': 'Computer Vision',
    'cv': 'Computer Vision',
    'yolo': 'Computer Vision',
    'opencv': 'Computer Vision',
    'nlp': 'NLP',
    'natural-language': 'NLP',
    'transformers': 'NLP',
    'text-classification': 'NLP',
    'react': 'Frontend Framework',
    'nextjs': 'Frontend Framework',
    'next-js': 'Frontend Framework',
    'vue': 'Frontend Framework',
    'svelte': 'Frontend Framework',
    'angular': 'Frontend Framework',
    'remix': 'Frontend Framework',
    'astro': 'Frontend Framework',
    'api': 'API',
    'rest': 'API',
    'graphql': 'API',
    'fastapi': 'API',
    'grpc': 'API',
    'docker': 'DevOps',
    'kubernetes': 'DevOps',
    'k8s': 'DevOps',
    'devops': 'DevOps',
    'terraform': 'DevOps',
    'ansible': 'DevOps',
    'ci': 'DevOps',
    'github-actions': 'DevOps',
    'firebase': 'Database',
    'supabase': 'Database',
    'mongodb': 'Database',
    'postgres': 'Database',
    'postgresql': 'Database',
    'mysql': 'Database',
    'redis': 'Database',
    'sqlite': 'Database',
    'ml': 'Machine Learning',
    'machine-learning': 'Machine Learning',
    'deep-learning': 'Machine Learning',
    'pytorch': 'Machine Learning',
    'tensorflow': 'Machine Learning',
    'sklearn': 'Machine Learning',
    'scikit-learn': 'Machine Learning',
    'automation': 'Automation',
    'workflow': 'Automation',
    'n8n': 'Automation',
    'zapier': 'Automation',
    'open-source': 'Open Source',
    'opensource': 'Open Source',
    'cli': 'CLI Tool',
    'terminal': 'CLI Tool',
    'mobile': 'Mobile',
    'ios': 'Mobile',
    'android': 'Mobile',
    'react-native': 'Mobile',
    'flutter': 'Mobile',
    'blockchain': 'Web3',
    'web3': 'Web3',
    'solidity': 'Web3',
    'ethereum': 'Web3',
    'data': 'Data Science',
    'analytics': 'Data Science',
    'pandas': 'Data Science',
    'jupyter': 'Data Science',
    'security': 'Security',
    'auth': 'Security',
    'oauth': 'Security',
    'jwt': 'Security',
    'testing': 'Testing',
    'jest': 'Testing',
    'pytest': 'Testing',
    'game': 'Game Dev',
    'gamedev': 'Game Dev',
    'unity': 'Game Dev',
}

# ── README keyword → tag ──────────────────────────────────────────────────────

README_KEYWORD_MAP: list[tuple[list[str], str]] = [
    # AI Core
    (['large language model', 'llm', 'language model'], 'Large Language Models'),
    (['gpt', 'openai', 'chatgpt'], 'OpenAI'),
    (['claude', 'anthropic'], 'Anthropic / Claude'),
    (['gemini', 'vertex ai', 'google ai'], 'Google AI'),
    (['retrieval augmented', 'retrieval-augmented'], 'RAG'),
    (['vector database', 'vector db', 'vector store', 'embedding store'], 'Vector Database'),
    (['embedding', 'embeddings'], 'Embeddings'),
    (['fine-tun', 'finetuning', 'fine tuning', 'qlora'], 'Fine-Tuning'),
    (['reinforcement learning', 'rlhf', 'reward model'], 'Reinforcement Learning'),
    (['ai agent', 'agentic', 'autonomous agent', 'multi-agent', 'agent framework', 'agent workflow'], 'AI Agents'),
    (['mcp', 'model context protocol'], 'MCP'),
    # AI Frameworks
    (['langchain', 'lang chain'], 'LangChain'),
    (['langgraph', 'lang graph'], 'LangGraph'),
    (['llamaindex', 'llama index', 'llama_index'], 'LlamaIndex'),
    (['crewai', 'crew ai'], 'CrewAI'),
    (['autogen', 'auto gen'], 'AutoGen'),
    (['hugging face', 'huggingface', 'transformers library'], 'HuggingFace'),
    (['ollama'], 'Ollama'),
    (['vllm', 'llm serving', 'model serving'], 'LLM Serving'),
    (['onnx', 'tensorrt', 'model optimization'], 'Model Optimization'),
    (['lora ', 'qlora', 'peft'], 'LoRA / PEFT'),
    (['comfyui', 'comfy ui'], 'ComfyUI'),
    (['openai whisper'], 'Whisper'),
    (['n8n', 'make.com', 'zapier'], 'No-Code Automation'),
    (['multiagent', 'multi-agent', 'agent swarm'], 'Multi-Agent'),
    (['function calling', 'tool calling', 'tool use'], 'Tool Use'),
    (['structured output', 'json mode', 'pydantic'], 'Structured Output'),
    (['benchmark', 'evaluation', 'evals', 'llm eval'], 'Evals'),
    (['synthetic data', 'data generation'], 'Synthetic Data'),
    (['quantization', 'gguf', 'quantized'], 'Quantization'),
    (['inference server', 'inference engine'], 'Inference'),
    (['long context', 'context extension'], 'Long Context'),
    (['agent memory', 'persistent memory'], 'Agent Memory'),
    (['chain of thought', 'task planning'], 'Planning / CoT'),
    (['simulator', 'simulation', 'gazebo'], 'Simulation'),
    (['slam algorithm', 'localization and mapping'], 'SLAM'),
    (['humanoid', 'bipedal', 'legged robot'], 'Humanoid Robotics'),
    (['diffusion policy', 'imitation learning', 'robot learning'], 'Robot Learning'),
    (['prompt engineering', 'prompt template', 'system prompt'], 'Prompt Engineering'),
    (['context engineering', 'context window'], 'Context Engineering'),
    # ML & Deep Learning
    (['machine learning', 'ml model', 'sklearn', 'scikit-learn'], 'Machine Learning'),
    (['deep learning', 'neural network', 'deep neural'], 'Deep Learning'),
    (['transformer', 'transformers', 'attention mechanism', 'gpt-2'], 'Transformers'),
    (['pytorch', 'torch'], 'PyTorch'),
    (['tensorflow', 'tf.keras'], 'TensorFlow'),
    (['keras'], 'Keras'),
    (['flax'], 'JAX'),
    (['mlops', 'ml pipeline', 'model deployment', 'model serving'], 'MLOps'),
    (['cuda', 'gpu programming', 'gpu performance', 'triton kernel'], 'GPU / CUDA'),
    # Generative AI
    (['diffusion model', 'stable diffusion', 'comfyui', 'image generation', 'text to image'], 'Image Generation'),
    (['video generation', 'video model', 'video diffusion', 'ltx-video'], 'Video Generation'),
    (['text to speech', 'speech synthesis', 'voice cloning', 'voice generation'], 'Text to Speech'),
    (['speech to text', 'speech recognition', 'transcription', 'whisper'], 'Speech to Text'),
    (['music generation', 'audio generation', 'audio model', 'sound synthesis'], 'Music / Audio AI'),
    (['multimodal', 'vision language', 'image understanding'], 'Multimodal AI'),
    # Computer Vision & Spatial
    (['computer vision', 'object detection', 'image segmentation', 'yolo', 'opencv'], 'Computer Vision'),
    (['point cloud', 'lidar', 'depth estimation', '3d reconstruction', 'nerf'], 'Point Cloud / 3D Vision'),
    (['robotics', 'ros ', 'robot operating', 'robotic arm', 'autonomous robot'], 'Robotics'),
    (['robot arm', 'robotic arm', 'gripper', 'actuator'], 'Robot Arms'),
    (['autonomous vehicle', 'self-driving', 'slam algorithm', 'autonomous robot', 'autonomous system'], 'Autonomous Systems'),
    (['extended reality', 'webxr', 'mixed reality', 'spatial computing'], 'XR / Spatial Computing'),
    (['virtual reality', 'vr headset', 'oculus', 'meta quest'], 'Virtual Reality'),
    (['augmented reality', 'arkit', 'arcore'], 'Augmented Reality'),
    (['volumetric', 'immersive media', 'immersive experience'], 'Immersive Media'),
    # Languages & Frameworks
    (['python'], 'Python'),
    (['typescript', 'tsx'], 'TypeScript'),
    (['javascript'], 'JavaScript'),
    (['rust'], 'Rust'),
    (['golang'], 'Go'),
    (['java '], 'Java'),
    (['c++', 'cpp'], 'C++'),
    (['react', 'nextjs', 'next.js'], 'React / Next.js'),
    (['fastapi', 'flask', 'django'], 'Python Web Framework'),
    (['node.js', 'nodejs', 'express'], 'Node.js'),
    # Infrastructure & DevOps
    (['docker', 'dockerfile', 'containeriz'], 'Docker'),
    (['kubernetes', 'k8s', 'helm chart'], 'Kubernetes'),
    (['rest api', 'api endpoint', 'api reference', 'swagger', 'openapi', 'http api'], 'API'),
    (['graphql'], 'GraphQL'),
    (['database', 'postgresql', 'mysql', 'sqlite'], 'Database'),
    (['redis', 'caching', 'cache layer', 'memcached'], 'Caching'),
    (['amazon web services', 's3', 'ec2'], 'AWS'),
    (['google cloud', 'gcp', 'firebase', 'vertex'], 'Google Cloud'),
    (['automation', 'workflow automation', 'n8n', 'zapier'], 'Automation'),
    (['command-line', 'command line interface', 'terminal tool', 'cli tool', 'command line tool'], 'CLI Tool'),
    # Knowledge & Learning
    (['tutorial', 'beginner', 'getting started', 'introduction to'], 'Tutorial'),
    (['course', 'curriculum', 'lesson', 'lecture'], 'Course'),
    (['roadmap', 'learning path'], 'Roadmap'),
    (['cheat sheet', 'cheatsheet', 'quick reference'], 'Cheat Sheet'),
    (['awesome ', 'curated list', 'collection of'], 'Curated List'),
    (['interview', 'interview prep', 'interview question'], 'Interview Prep'),
    (['research paper', 'arxiv', 'paper implementation'], 'Research / Papers'),
    (['open source'], 'Open Source'),
    # Domains
    (['fintech', 'financial', 'payment', 'banking'], 'FinTech'),
    (['healthcare', 'medical', 'clinical', 'biomedical'], 'Healthcare AI'),
    (['music', 'song', 'audio production', 'daw'], 'Music Tech'),
    (['unity game', 'unreal engine', 'game engine', 'pygame', 'godot', 'gaming'], 'Game Dev'),
    (['security', 'cybersecurity', 'vulnerability', 'penetration'], 'Security'),
    (['blockchain', 'web3', 'solidity', 'smart contract'], 'Web3'),
    (['mobile', 'ios', 'android', 'react native', 'flutter'], 'Mobile'),
    (['data science', 'data analysis', 'pandas', 'jupyter'], 'Data Science'),
    (['knowledge graph', 'graph rag', 'graphrag'], 'Knowledge Graph'),
    (['streaming', 'real-time', 'websocket'], 'Real-Time / Streaming'),
    # Specific AI frameworks
    (['dspy', 'ds-py'], 'DSPy'),
    (['instructor library', 'instructor-python'], 'Instructor'),
    (['microsoft guidance', 'guidance ai'], 'Guidance'),
    (['semantic kernel'], 'Semantic Kernel'),
    (['haystack', 'deepset haystack'], 'Haystack'),
    (['litellm', 'lite llm'], 'LiteLLM'),
    (['sglang', 'sg-lang'], 'SGLang'),
    (['unsloth'], 'Unsloth'),
    (['axolotl training', 'axolotl finetuning'], 'Axolotl'),
    (['mergekit', 'merge kit', 'model merging'], 'MergeKit'),
    (['open-webui', 'openwebui', 'open webui'], 'Open WebUI'),
    (['flowise'], 'Flowise'),
    (['vllm', 'v-llm inference'], 'vLLM'),
    (['text-generation-inference', 'huggingface tgi'], 'TGI'),
    (['triton inference server', 'nvidia triton'], 'Triton'),
    (['tensorrt-llm', 'tensorrt llm'], 'TensorRT'),
    (['llama.cpp', 'llama cpp', 'llamacpp'], 'llama.cpp'),
    (['exllamav2', 'exllama v2'], 'ExLlama'),
    (['gpt4all', 'gpt-4-all', 'gpt 4 all'], 'GPT4All'),
    (['privategpt', 'private gpt chat'], 'PrivateGPT'),
    (['continue.dev', 'continuedev'], 'Continue.dev'),
    (['aider coding', 'aider-chat'], 'Aider'),
    (['swe-agent', 'sweagent'], 'SWE-Agent'),
    (['opendevin', 'open devin'], 'OpenDevin'),
    # Evals and benchmarking
    (['benchmarking', 'leaderboard ranking', 'model leaderboard'], 'Benchmarking'),
    (['mmlu benchmark'], 'MMLU'),
    (['humaneval benchmark', 'human-eval'], 'HumanEval'),
    (['red-teaming', 'red team attack', 'redteaming'], 'Red Teaming'),
    (['deepeval', 'deep-eval framework'], 'DeepEval'),
    (['ragas evaluation', 'ragas framework'], 'RAGAS'),
    # Observability
    (['langsmith', 'lang smith tracing'], 'LangSmith'),
    (['arize phoenix', 'phoenix tracing'], 'Phoenix'),
    (['mlflow tracking', 'mlflow experiment'], 'MLflow'),
    (['weights and biases', 'wandb logging', 'weights & biases'], 'Weights & Biases'),
    (['opentelemetry tracing', 'otel tracing'], 'Tracing'),
    (['model monitoring', 'llm monitoring system'], 'Monitoring'),
    # Training specific
    (['rlhf training', 'reinforcement learning from human feedback'], 'RLHF'),
    (['direct preference optimization', 'dpo training'], 'DPO'),
    (['grpo training', 'group relative policy optimization'], 'GRPO'),
    (['knowledge distillation', 'model distillation'], 'Distillation'),
    (['deepspeed training', 'deepspeed zero'], 'DeepSpeed'),
    (['fsdp training', 'fully sharded data parallel'], 'FSDP'),
    (['trl library', 'transformer reinforcement learning'], 'TRL'),
    (['axolotl'], 'Axolotl'),
    # Inference optimization
    (['speculative decoding', 'speculative sampling'], 'Speculative Decoding'),
    (['kv cache', 'kv-cache', 'key value cache'], 'KV Cache'),
    (['continuous batching', 'dynamic batching'], 'Batching'),
    # RAG specific
    (['reranking', 'cross-encoder rerank', 're-ranking'], 'Reranking'),
    (['hybrid search bm25', 'sparse dense retrieval'], 'Hybrid Search'),
    (['document parsing', 'pdf parsing', 'document extraction'], 'Document Processing'),
    (['text chunking', 'text splitting', 'recursive splitter'], 'Chunking'),
    (['semantic search', 'dense retrieval'], 'Semantic Search'),
    # Computer vision specific
    (['object detection model', 'yolo detection', 'yolov'], 'Object Detection'),
    (['image segmentation', 'semantic segmentation', 'instance segmentation'], 'Segmentation'),
    (['monocular depth', 'depth estimation model'], 'Depth Estimation'),
    (['pose estimation', 'keypoint detection', 'human pose'], 'Pose Estimation'),
    (['neural radiance field', 'nerf rendering', 'gaussian splatting', '3d reconstruction'], '3D Reconstruction'),
    # Robotics specific
    (['ros2 ', 'ros 2 ', 'robot operating system'], 'ROS'),
    (['motion planning algorithm', 'path planning robot', 'trajectory optimization'], 'Motion Planning'),
    (['robot grasping', 'pick and place', 'robot manipulation'], 'Grasping'),
    (['sim-to-real', 'sim2real transfer', 'domain randomization'], 'Sim-to-Real'),
    # Generative media specific
    (['stable diffusion', 'sdxl', 'sd3 ', 'stablediffusion'], 'Stable Diffusion'),
    (['controlnet', 'control net conditioning'], 'ControlNet'),
    (['voice cloning model', 'voice synthesis model', 'tts clone'], 'Voice Cloning'),
    (['musicgen', 'music generation model', 'audio generation model'], 'Music Generation'),
    # MLOps specific
    (['dvc data versioning', 'data version control'], 'DVC'),
    (['zenml pipeline', 'zen ml'], 'ZenML'),
    (['prefect workflow', 'prefect flow'], 'Prefect'),
    (['apache airflow', 'airflow dag'], 'Airflow'),
    (['ray cluster', 'ray tune', 'ray distributed'], 'Ray'),
    (['feature store', 'feast feature'], 'Feature Store'),
    (['model registry', 'model versioning system'], 'Model Registry'),
    # Security and safety
    (['ai safety research', 'model safety'], 'AI Safety'),
    (['adversarial attack', 'adversarial robustness', 'adversarial example'], 'Adversarial'),
    (['watermarking model', 'ai watermark', 'content provenance'], 'Watermarking'),
    (['differential privacy', 'federated learning', 'privacy preserving ml'], 'Privacy'),
    # XR specific
    (['webxr api', 'web xr'], 'WebXR'),
    (['arkit framework', 'ios arkit'], 'ARKit'),
    (['arcore framework', 'android arcore'], 'ARCore'),
    (['meta quest', 'oculus quest', 'quest 3 '], 'Meta Quest'),
    (['apple vision pro', 'visionos', 'apple vision'], 'Apple Vision'),
    # Data Science
    (['numpy', 'numerical computing'], 'NumPy'),
    (['data visualization', 'matplotlib', 'plotly chart'], 'Visualization'),
    (['data engineering pipeline', 'etl pipeline', 'data pipeline'], 'Data Engineering'),
    (['statistics', 'statistical analysis', 'statistical modeling'], 'Statistics'),
    # Observability (second pass, specific tools)
    (['langfuse'], 'Langfuse'),
    (['openllmetry', 'open llmetry'], 'OpenLLMetry'),
    (['openlit'], 'OpenLIT'),
    (['helicone'], 'Helicone'),
    (['arize', 'phoenix arize'], 'Phoenix'),
    (['traceloop'], 'Traceloop'),
    (['weights biases', 'wandb', 'w&b'], 'Weights & Biases'),
    (['mlflow'], 'MLflow'),
    (['opentelemetry', 'otel'], 'OpenTelemetry'),
    # Evals (second pass)
    (['deepeval', 'deep eval'], 'DeepEval'),
    (['ragas'], 'RAGAS'),
    (['promptfoo', 'prompt foo'], 'PromptFoo'),
    (['lm-evaluation-harness', 'lm eval harness'], 'LM Eval Harness'),
    (['evals framework', 'llm eval', 'model eval'], 'Evals'),
    (['red team', 'redteam', 'red-team'], 'Red Teaming'),
    (['garak'], 'Garak'),
    (['pyrit'], 'PyRIT'),
    (['benchmark', 'benchmarking', 'leaderboard'], 'Benchmarking'),
    (['mmlu'], 'MMLU'),
    (['humaneval', 'human eval'], 'HumanEval'),
    # Inference & Serving (second pass)
    (['vllm', 'v-llm'], 'vLLM'),
    (['sglang', 'sg-lang'], 'SGLang'),
    (['text-generation-inference', 'tgi'], 'TGI'),
    (['triton inference', 'triton server'], 'Triton'),
    (['tensorrt', 'tensor rt', 'trt'], 'TensorRT'),
    (['onnx'], 'ONNX'),
    (['llama.cpp', 'llamacpp', 'llama cpp'], 'llama.cpp'),
    (['llamafile'], 'Llamafile'),
    (['exllamav2', 'exllama'], 'ExLlama'),
    (['pageattention', 'paged attention', 'continuous batching'], 'vLLM'),
    (['speculative decoding', 'speculative sampling'], 'Speculative Decoding'),
    (['kv cache', 'kv-cache'], 'KV Cache'),
    (['model serving', 'llm serving', 'inference server'], 'LLM Serving'),
    (['quantization', 'quantized', 'gguf', 'ggml'], 'Quantization'),
    # Fine-tuning & Training (second pass)
    (['unsloth'], 'Unsloth'),
    (['axolotl'], 'Axolotl'),
    (['trl', 'transformer reinforcement learning'], 'TRL'),
    (['torchtune', 'torch tune'], 'TorchTune'),
    (['mergekit', 'merge kit', 'model merging'], 'MergeKit'),
    (['lora', 'lo-ra', 'low-rank adaptation'], 'LoRA / PEFT'),
    (['qlora', 'q-lora'], 'LoRA / PEFT'),
    (['peft'], 'LoRA / PEFT'),
    (['rlhf', 'reinforcement learning from human feedback'], 'RLHF'),
    (['dpo', 'direct preference optimization'], 'DPO'),
    (['grpo', 'group relative policy'], 'GRPO'),
    (['deepspeed'], 'DeepSpeed'),
    (['fsdp', 'fully sharded'], 'FSDP'),
    (['synthetic data', 'data synthesis'], 'Synthetic Data'),
    (['distillation', 'knowledge distillation'], 'Distillation'),
    # Structured Output & Reliability
    (['instructor library', 'jxnl instructor'], 'Instructor'),
    (['outlines text', 'dottxt outlines'], 'Outlines'),
    (['guidance microsoft', 'microsoft guidance'], 'Guidance'),
    (['guardrails ai', 'guardrails library'], 'Guardrails'),
    (['nemo guardrails', 'nvidia guardrails'], 'NeMo Guardrails'),
    (['structured output', 'json mode', 'json schema output'], 'Structured Output'),
    (['function calling', 'tool calling', 'tool use'], 'Tool Use'),
    (['pydantic'], 'Pydantic'),
    # Agent Frameworks (second pass)
    (['langgraph', 'lang graph'], 'LangGraph'),
    (['dspy', 'ds-py'], 'DSPy'),
    (['semantic kernel', 'semantickernel'], 'Semantic Kernel'),
    (['haystack deepset', 'deepset haystack'], 'Haystack'),
    (['litellm', 'lite llm'], 'LiteLLM'),
    (['agno framework'], 'Agno'),
    (['letta', 'memgpt'], 'Letta / MemGPT'),
    (['mem0', 'memory layer'], 'Mem0'),
    (['openai swarm', 'swarm agents'], 'Swarm'),
    (['openai agents sdk'], 'OpenAI Agents SDK'),
    (['multi-agent', 'multiagent', 'agent swarm'], 'Multi-Agent'),
    (['agent memory', 'persistent memory', 'long term memory'], 'Agent Memory'),
    (['planning', 'chain of thought', 'cot', 'tree of thought'], 'Planning / CoT'),
    (['context engineering', 'context management'], 'Context Engineering'),
    # RAG specific (second pass)
    (['chroma', 'chromadb'], 'Chroma'),
    (['qdrant'], 'Qdrant'),
    (['milvus'], 'Milvus'),
    (['weaviate'], 'Weaviate'),
    (['pinecone'], 'Pinecone'),
    (['pgvector', 'pg vector'], 'pgvector'),
    (['rerank', 'reranking', 'cross-encoder', 'cohere rerank'], 'Reranking'),
    (['hybrid search', 'bm25'], 'Hybrid Search'),
    (['graphrag', 'graph rag', 'microsoft graphrag'], 'GraphRAG'),
    (['document parsing', 'pdf parsing', 'unstructured'], 'Document Processing'),
    # Coding Assistants
    (['openhands', 'open hands', 'opendevin'], 'OpenHands'),
    (['cline', 'cline vscode'], 'Cline'),
    (['continue dev', 'continuedev'], 'Continue.dev'),
    (['aider'], 'Aider'),
    (['swe-agent', 'sweagent'], 'SWE-Agent'),
    (['claude code', 'claudecode'], 'Claude Code'),
    (['gemini cli', 'geminicli'], 'Gemini CLI'),
    (['kilocode'], 'Kilocode'),
    # Visual / No-code
    (['langflow'], 'Langflow'),
    (['flowise'], 'Flowise'),
    (['n8n'], 'n8n'),
    (['comfyui', 'comfy ui'], 'ComfyUI'),
    (['automatic1111', 'stable diffusion webui'], 'SD WebUI'),
    # Models
    (['deepseek'], 'DeepSeek'),
    (['qwen', 'qwen2'], 'Qwen'),
    (['llama3', 'llama 3', 'meta llama'], 'Llama'),
    (['mistral', 'mixtral'], 'Mistral'),
    (['phi-3', 'phi3', 'microsoft phi'], 'Phi'),
    (['gemma', 'google gemma'], 'Gemma'),
    (['claude', 'anthropic claude'], 'Claude'),
    (['gpt-4', 'gpt4', 'openai gpt'], 'GPT'),
    (['open-r1', 'openr1', 'reasoning model'], 'Reasoning Models'),
    # MLOps (second pass)
    (['dvc', 'data version control'], 'DVC'),
    (['zenml'], 'ZenML'),
    (['prefect'], 'Prefect'),
    (['airflow', 'apache airflow'], 'Airflow'),
    (['ray cluster', 'ray tune', 'ray serve'], 'Ray'),
    (['kubeflow'], 'Kubeflow'),
    (['feast feature store'], 'Feature Store'),
    # Security
    (['prompt injection', 'jailbreak', 'jail break'], 'Prompt Injection'),
    (['ai safety', 'model safety', 'alignment'], 'AI Safety'),
    (['watermark', 'watermarking'], 'Watermarking'),
    (['federated learning', 'differential privacy'], 'Privacy-Preserving AI'),
    # Robotics (second pass, stricter)
    (['ros2', 'ros 2', 'robot operating system 2'], 'ROS 2'),
    ([' ros ', 'robot operating system'], 'ROS'),
    (['motion planning', 'path planning', 'trajectory planning'], 'Motion Planning'),
    (['grasping', 'manipulation', 'pick and place'], 'Grasping'),
    (['humanoid robot', 'bipedal robot', 'legged robot'], 'Humanoid Robotics'),
    (['sim-to-real', 'sim2real'], 'Sim-to-Real'),
    (['diffusion policy', 'imitation learning', 'behavior cloning'], 'Robot Learning'),
    (['slam', 'simultaneous localization and mapping'], 'SLAM'),
    # XR (second pass, stricter)
    (['webxr', 'web xr'], 'WebXR'),
    (['arkit', 'ar kit'], 'ARKit'),
    (['arcore', 'ar core'], 'ARCore'),
    (['meta quest', 'oculus quest'], 'Meta Quest'),
    (['apple vision pro', 'visionos'], 'Apple Vision Pro'),
    (['augmented reality'], 'Augmented Reality'),
    (['virtual reality'], 'Virtual Reality'),
    (['mixed reality', 'extended reality'], 'Mixed Reality'),
    # Data science (second pass)
    (['pandas', 'dataframe'], 'Pandas'),
    (['jupyter notebook', 'ipynb'], 'Jupyter'),
    (['matplotlib', 'seaborn', 'plotly visualization'], 'Data Visualization'),
    (['scikit-learn', 'sklearn'], 'Scikit-learn'),
    (['numpy'], 'NumPy'),
    (['apache spark', 'pyspark'], 'Spark'),
    # Cloud & Platforms
    (['vertex ai', 'vertexai'], 'Vertex AI'),
    (['sagemaker', 'amazon sagemaker'], 'SageMaker'),
    (['azure openai', 'azure ai'], 'Azure AI'),
    (['bedrock', 'amazon bedrock'], 'AWS Bedrock'),
    (['hugging face', 'huggingface'], 'HuggingFace'),
]


def _matches_keyword(text: str, keyword: str) -> bool:
    """Word-boundary match — never matches substrings. Case-insensitive."""
    escaped = re.escape(keyword)
    pattern = re.compile(r'(?<![a-zA-Z0-9])' + escaped + r'(?![a-zA-Z0-9])', re.IGNORECASE)
    return bool(pattern.search(text))


def extract_tags_from_readme(readme_text: str) -> list[str]:
    """Extract enriched tags from README text using keyword matching."""
    tags: set[str] = set()
    for keywords, tag in README_KEYWORD_MAP:
        if any(_matches_keyword(readme_text, kw) for kw in keywords):
            tags.add(tag)
    return list(tags)


def generate_meta_tags(
    language: str | None,
    topics: list[str],
    stars: int,
    updated_at: str,
    is_fork: bool,
    is_archived: bool,
) -> list[str]:
    """Generate tags from GitHub metadata (language, topics, stars, dates)."""
    tags: set[str] = set()

    if language and language in LANGUAGE_TAGS:
        for tag in LANGUAGE_TAGS[language]:
            tags.add(tag)

    for topic in topics:
        normalized = topic.lower().replace(' ', '-')
        if normalized in TOPIC_TAGS:
            tags.add(TOPIC_TAGS[normalized])

    if stars > 1000:
        tags.add('Popular')

    try:
        updated = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
        days_since = (datetime.now(timezone.utc) - updated).days
        if days_since < 30:
            tags.add('Active')
        elif days_since > 365:
            tags.add('Inactive')
    except Exception:
        pass

    tags.add('Forked' if is_fork else 'Built by Me')

    if is_archived:
        tags.add('Archived')

    return list(tags)


def enrich_tags(
    language: str | None,
    topics: list[str],
    stars: int,
    updated_at: str,
    is_fork: bool,
    is_archived: bool,
    readme_text: str | None = None,
) -> list[str]:
    """Full tag enrichment: meta + README. Returns sorted, deduplicated tags."""
    meta = generate_meta_tags(language, topics, stars, updated_at, is_fork, is_archived)
    readme = extract_tags_from_readme(readme_text) if readme_text else []
    return sorted(set(meta + readme))
