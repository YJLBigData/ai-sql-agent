from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"
KNOWLEDGE_DIR = PROJECT_ROOT / "knowledge"
SQL_DIR = PROJECT_ROOT / "sql"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
WEB_DIR = PROJECT_ROOT / "web"
LOG_DIR = PROJECT_ROOT / "logs"

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class MySQLSettings:
    host: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    user: str = os.getenv("MYSQL_USER", "root")
    password: str = os.getenv("MYSQL_PASSWORD", "toor")
    database: str = os.getenv("MYSQL_DATABASE", "sql_agent")
    charset: str = "utf8mb4"


@dataclass(frozen=True)
class SeedSettings:
    user_count: int = int(os.getenv("SQL_AGENT_USER_COUNT", "200000"))
    order_count: int = int(os.getenv("SQL_AGENT_ORDER_COUNT", "100000"))
    refund_count: int = int(os.getenv("SQL_AGENT_REFUND_COUNT", "10000"))
    batch_size: int = int(os.getenv("SQL_AGENT_BATCH_SIZE", "5000"))
    random_seed: int = int(os.getenv("SQL_AGENT_RANDOM_SEED", "20260315"))


@dataclass(frozen=True)
class ProviderSettings:
    name: str
    label: str
    base_url: str
    api_key_env: str
    default_model: str
    model_options: tuple[str, ...]


@dataclass(frozen=True)
class EmbeddingSettings:
    enabled: bool = os.getenv("LOCAL_EMBEDDING_ENABLED", "1") != "0"
    backend: str = os.getenv("LOCAL_EMBEDDING_BACKEND", "hybrid")
    model_name: str = os.getenv("LOCAL_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5")
    remote_model_name: str = os.getenv("LOCAL_EMBEDDING_REMOTE_MODEL", "")
    base_url: str = os.getenv("LOCAL_EMBEDDING_BASE_URL", "http://127.0.0.1:8000/v1")
    api_key: str = os.getenv("LOCAL_EMBEDDING_API_KEY", "EMPTY")
    request_timeout: float = float(os.getenv("LOCAL_EMBEDDING_TIMEOUT", "12"))
    cache_dir: Path = DATA_DIR / "embedding_models"
    index_dir: Path = DATA_DIR / "vector_index"


@dataclass(frozen=True)
class LocalLLMSettings:
    base_url: str = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    default_model: str = os.getenv("OLLAMA_DEFAULT_MODEL", "qwen3:8b")


def _provider_catalog() -> dict[str, ProviderSettings]:
    return {
        "bailian": ProviderSettings(
            name="bailian",
            label="阿里百炼",
            base_url=os.getenv("BAILIAN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            api_key_env="BAILIAN_API_KEY",
            default_model=os.getenv("BAILIAN_DEFAULT_MODEL", "qwen3-max"),
            model_options=("qwen3-max",),
        ),
        "deepseek": ProviderSettings(
            name="deepseek",
            label="DeepSeek",
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
            api_key_env="DEEPSEEK_API_KEY",
            default_model=os.getenv("DEEPSEEK_DEFAULT_MODEL", "deepseek-reasoner"),
            model_options=("deepseek-reasoner", "deepseek-chat"),
        ),
    }


@dataclass(frozen=True)
class AppSettings:
    project_root: Path = PROJECT_ROOT
    data_dir: Path = DATA_DIR
    knowledge_dir: Path = KNOWLEDGE_DIR
    sql_dir: Path = SQL_DIR
    output_dir: Path = OUTPUT_DIR
    web_dir: Path = WEB_DIR
    log_dir: Path = LOG_DIR
    mysql: MySQLSettings = field(default_factory=MySQLSettings)
    seed: SeedSettings = field(default_factory=SeedSettings)
    embedding: EmbeddingSettings = field(default_factory=EmbeddingSettings)
    local_llm: LocalLLMSettings = field(default_factory=LocalLLMSettings)
    providers: dict[str, ProviderSettings] = field(default_factory=_provider_catalog)
    default_provider: str = os.getenv("DEFAULT_LLM_PROVIDER", "bailian")
    app_host: str = os.getenv("APP_HOST", "127.0.0.1")
    app_port: int = int(os.getenv("APP_PORT", "8502"))

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.web_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.embedding.cache_dir.mkdir(parents=True, exist_ok=True)
        self.embedding.index_dir.mkdir(parents=True, exist_ok=True)

    def get_provider(self, provider_name: str) -> ProviderSettings:
        provider = self.providers.get(provider_name)
        if provider is None:
            raise ValueError(f"不支持的模型供应商: {provider_name}")
        return provider

    def get_api_key(self, provider_name: str) -> str:
        provider = self.get_provider(provider_name)
        api_key = os.getenv(provider.api_key_env)
        if not api_key:
            raise ValueError(f"环境变量 {provider.api_key_env} 未配置。")
        return api_key


def get_settings() -> AppSettings:
    settings = AppSettings()
    settings.ensure_directories()
    return settings
