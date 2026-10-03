"""Минимальная автономная CLI worker без зависимостей подготовки GeoJSON."""

import argparse
import json
from pathlib import Path

from .api import DEFAULT_BASE_URL, UrbanApi
from .distributed import (
    export_worker_result, request_worker_stop, worker_run, worker_status,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        add_help=False, prog="urban-import",
        description="Автономный worker URBAN Buildings Import"
    )
    parser.add_argument("-h", "--help", action="help", help="показать эту справку")
    parser._positionals.title = "команды"
    parser._optionals.title = "параметры"
    commands = parser.add_subparsers(dest="command", required=True)
    def command(name: str, help_text: str):
        result = commands.add_parser(name, add_help=False, help=help_text,
                                     description=help_text)
        result.add_argument("-h", "--help", action="help", help="показать эту справку")
        result._positionals.title = "позиционные аргументы"
        result._optionals.title = "параметры"
        return result

    run = command("worker-run", "проверить или продолжить worker-план")
    run.add_argument("--bundle", type=Path, required=True)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--once", action="store_true", help="не ждать на барьере")
    run.add_argument("--base-url", default=DEFAULT_BASE_URL)
    run.add_argument("--delay", type=float, default=0.4)
    run.add_argument("--timeout", type=float, default=90)
    status = command("worker-status", "показать локальное состояние")
    status.add_argument("--bundle", type=Path, required=True)
    stop = command("worker-stop", "мягко остановить worker на безопасной точке")
    stop.add_argument("--bundle", type=Path, required=True)
    export = command("worker-export", "создать компактный result ZIP")
    export.add_argument("--bundle", type=Path, required=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "worker-status":
            result = worker_status(args.bundle)
        elif args.command == "worker-stop":
            result = request_worker_stop(args.bundle)
        elif args.command == "worker-export":
            archive, complete = export_worker_result(args.bundle)
            result = {"archive": str(archive), "complete": complete}
            print(json.dumps(result, ensure_ascii=False, indent=2))
            if not complete:
                raise SystemExit(4)
            return
        else:
            api = UrbanApi(args.base_url, delay=args.delay, timeout=args.timeout)
            result = worker_run(args.bundle, api, dry_run=args.dry_run,
                                wait=not args.once)
            if result.get("stopped") or (args.once and result.get("waiting")):
                print(json.dumps(result, ensure_ascii=False, indent=2))
                raise SystemExit(3)
    except (RuntimeError, OSError, ValueError) as exc:
        parser.exit(2, f"Ошибка: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
