from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sql_ai_copilot.agent.sql_copilot import SQLClarificationRequired
from sql_ai_copilot.governance.case_factory import build_default_evaluation_cases
from sql_ai_copilot.logging_utils import get_logger


@dataclass(frozen=True)
class EvaluationCase:
    case_id: str
    question: str
    task_mode: str
    sql_engine: str
    engine_mode: str
    provider: str
    execute: bool
    must_contain_sql: tuple[str, ...]
    min_row_count: int | None


class RegressionEvaluator:
    def __init__(self, agent, output_dir: Path) -> None:
        self.agent = agent
        self.output_dir = output_dir
        self.logger = get_logger("evaluator")

    def run(
        self,
        case_file: Path,
        local_model: str,
        online_model: str | None = None,
        limit: int | None = None,
        provider_override: str | None = None,
        engine_mode_override: str | None = None,
    ) -> dict[str, object]:
        cases = self._load_cases(case_file)
        if limit is not None:
            cases = cases[:limit]

        results: list[dict[str, object]] = []
        clarification_count = 0
        for case in cases:
            try:
                result = self.agent.run(
                    case.question,
                    provider_override or case.provider,
                    online_model or "",
                    execute=case.execute,
                    task_mode=case.task_mode,
                    sql_engine=case.sql_engine,
                    engine_mode=engine_mode_override or case.engine_mode,
                    local_model_name=local_model,
                    online_model_name=online_model or "",
                )
                sql = result.sql.lower()
                passed = all(fragment.lower() in sql for fragment in case.must_contain_sql)
                if case.min_row_count is not None:
                    row_count = len(result.rows or [])
                    passed = passed and row_count >= case.min_row_count
                trace_usage = result.trace.get("usage", {})
                results.append(
                    {
                        "case_id": case.case_id,
                        "question": case.question,
                        "success": passed,
                        "route": result.trace.get("route"),
                        "elapsed_ms": result.trace.get("elapsed_ms"),
                        "prompt_tokens": trace_usage.get("prompt_tokens", 0),
                        "completion_tokens": trace_usage.get("completion_tokens", 0),
                        "total_tokens": trace_usage.get("total_tokens", 0),
                    }
                )
            except SQLClarificationRequired as exc:
                clarification_count += 1
                results.append(
                    {
                        "case_id": case.case_id,
                        "question": case.question,
                        "success": False,
                        "clarification_required": True,
                        "error": exc.message,
                        "route": "clarification",
                        "elapsed_ms": None,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "case_id": case.case_id,
                        "question": case.question,
                        "success": False,
                        "error": str(exc),
                        "route": "failed",
                        "elapsed_ms": None,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    }
                )

        success_count = sum(1 for item in results if item["success"])
        executed_count = sum(1 for item in results if item.get("route") not in {"failed", None, "clarification"})
        total_tokens = sum(int(item.get("total_tokens") or 0) for item in results)
        elapsed_values = [int(item["elapsed_ms"]) for item in results if item.get("elapsed_ms") is not None]
        report = {
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "case_count": len(results),
            "success_count": success_count,
            "success_rate": round(success_count / len(results), 4) if results else 0,
            "executable_rate": round(executed_count / len(results), 4) if results else 0,
            "clarification_rate": round(clarification_count / len(results), 4) if results else 0,
            "avg_token_cost": round(total_tokens / len(results), 2) if results else 0,
            "avg_elapsed_ms": round(sum(elapsed_values) / len(elapsed_values), 2) if elapsed_values else 0,
            "results": results,
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.output_dir / f"evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        failure_path = self.output_dir / "failure_bank.jsonl"
        with failure_path.open("a", encoding="utf-8") as handle:
            for item in results:
                if item["success"]:
                    continue
                handle.write(json.dumps(item, ensure_ascii=False) + "\n")
        self.logger.info("evaluation_done path=%s success_rate=%s", report_path, report["success_rate"])
        return report

    @staticmethod
    def _load_cases(case_file: Path) -> list[EvaluationCase]:
        if case_file.exists():
            payload = json.loads(case_file.read_text(encoding="utf-8"))
        else:
            payload = build_default_evaluation_cases()
        return [
            EvaluationCase(
                case_id=item["case_id"],
                question=item["question"],
                task_mode=item["task_mode"],
                sql_engine=item["sql_engine"],
                engine_mode=item["engine_mode"],
                provider=item["provider"],
                execute=bool(item["execute"]),
                must_contain_sql=tuple(item.get("must_contain_sql", [])),
                min_row_count=item.get("min_row_count"),
            )
            for item in payload
        ]
