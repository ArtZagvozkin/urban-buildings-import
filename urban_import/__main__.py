"""Единая командная строка обычного и распределённого импорта."""
import argparse
import json
from pathlib import Path
from .api import DEFAULT_BASE_URL, UrbanApi
from .config import ROOT, get_territory
from .distributed import build_worker_runtime, collect_worker_results, create_authorization, create_distributed_master_plan, export_worker_result, final_verify, package_distribution, worker_run, worker_status, request_worker_stop
from .prepare import prepare_territory
from .storage import ARTIFACTS, create_run, new_run_id, resolve_run, run_paths
from .workflow import apply_plan, create_plan, verify_run
from .results import RESULTS, ReadOnlyApi, create_restoration_plan, load_result, prepare_completed_result, read_restoration_plan, reproduce, status_markdown, verify_current
from .storage import read_json

def _add_connection_arguments(parser: argparse.ArgumentParser, delay: float=0.2) -> None:
    parser.add_argument('--base-url', default=DEFAULT_BASE_URL, help='адрес URBAN API')
    parser.add_argument('--delay', type=float, default=delay, help=f'минимальная пауза между запросами (по умолчанию {delay})')
    parser.add_argument('--timeout', type=float, default=90, help='таймаут чтения ответа, секунды (по умолчанию 90)')
    parser.add_argument('--direct-server-ip', help='необязательный прямой IP API; имя TLS сохраняется')
    parser.add_argument('--source-ip', help='локальный IP интерфейса для прямого подключения')

def _help(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('-h', '--help', action='help', help='показать эту справку')
    parser._positionals.title = 'позиционные аргументы'
    parser._optionals.title = 'параметры'

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False, prog='python -m urban_import', description='Проверка и контролируемая загрузка зданий в URBAN API')
    _help(parser)
    commands = parser.add_subparsers(dest='command', required=True, title='команды')
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('territory', help='стабильное имя территории')
    common.add_argument('--run', help='идентификатор запуска; по умолчанию активный')
    _add_connection_arguments(common)
    for name, description in [('prepare', 'проверить GeoJSON без изменения API'), ('preview', 'создать обычный backup и план только через GET'), ('apply', 'выполнить или продолжить обычный план'), ('verify', 'независимая GET-проверка обычного плана')]:
        command = commands.add_parser(name, parents=[common], add_help=False, help=description, description=description)
        _help(command)
        command.set_defaults(action=name)
        if name == 'prepare':
            command.add_argument('--live-boundary', action='store_true', help='для нового импорта получить новую границу API')
    package = commands.add_parser('distributed-package', add_help=False, help='создать GET-only master plan и четыре Windows-папки')
    package.add_argument('territory')
    package.add_argument('--run', help='идентификатор запуска')
    package.add_argument('--output', type=Path, help='каталог комплекта')
    package.add_argument('--runtime', type=Path, help='готовый PyInstaller onedir')
    _add_connection_arguments(package, delay=0.1)
    _help(package)
    package.set_defaults(action='distributed-package')
    authorize = commands.add_parser('distributed-authorize', add_help=False, help='после отдельного разрешения создать authorization.json')
    authorize.add_argument('--distribution', type=Path, required=True)
    authorize.add_argument('--confirm-complete-replacement', action='store_true')
    authorize.add_argument('--confirm-destructive-apply', action='store_true')
    _help(authorize)
    authorize.set_defaults(action='distributed-authorize')
    worker = commands.add_parser('worker-run', add_help=False, help='проверить или продолжить worker-план')
    worker.add_argument('--bundle', type=Path, required=True)
    worker.add_argument('--dry-run', action='store_true')
    worker.add_argument('--once', action='store_true', help='не ждать на барьере')
    _add_connection_arguments(worker, delay=0.4)
    _help(worker)
    worker.set_defaults(action='worker-run')
    status = commands.add_parser('worker-status', add_help=False, help='показать локальное состояние worker')
    status.add_argument('--bundle', type=Path, required=True)
    _help(status)
    status.set_defaults(action='worker-status')
    stop = commands.add_parser('worker-stop', add_help=False, help='мягко остановить worker на безопасной точке')
    stop.add_argument('--bundle', type=Path, required=True)
    _help(stop)
    stop.set_defaults(action='worker-stop')
    export = commands.add_parser('worker-export', add_help=False, help='создать компактный worker result ZIP')
    export.add_argument('--bundle', type=Path, required=True)
    _help(export)
    export.set_defaults(action='worker-export')
    collect = commands.add_parser('collect-results', add_help=False, help='проверить и объединить четыре результата')
    collect.add_argument('--distribution', type=Path, required=True)
    collect.add_argument('--results', type=Path, required=True)
    _help(collect)
    collect.set_defaults(action='collect-results')
    final = commands.add_parser('verify-final', add_help=False, help='финальная независимая проверка только через GET')
    final.add_argument('--distribution', type=Path, required=True)
    _add_connection_arguments(final, delay=0.1)
    _help(final)
    final.set_defaults(action='verify-final')
    for name, description in (('reproduce', 'offline: воспроизвести все тела запросов завершённого результата'), ('verify-current', 'новая GET-проверка канонического результата'), ('restore-preview', 'создать новый план восстановления текущего результата; только GET')):
        command = commands.add_parser(name, add_help=False, help=description, description=description)
        _help(command)
        command.add_argument('territory', help='имя территории или all')
        command.set_defaults(action=name)
        if name != 'reproduce':
            _add_connection_arguments(command)
        if name == 'verify-current':
            command.add_argument('--check-old-geometries', action='store_true', help='повторить дорогую GET-проверку каждого старого geometry ID')
        if name == 'restore-preview':
            command.add_argument('--output', type=Path, help='путь нового плана; для all задаётся каталог')
    status_doc = commands.add_parser('status-doc', add_help=False, help='сформировать или проверить docs/status.md по results/index.json')
    _help(status_doc)
    status_doc.add_argument('--check', action='store_true', help='только проверить соответствие')
    status_doc.set_defaults(action='status-doc')
    result_status = commands.add_parser('status', add_help=False, help='показать текущие подтверждённые результаты')
    _help(result_status)
    result_status.add_argument('territory', nargs='?', default='all', help='имя территории или all')
    result_status.set_defaults(action='status')
    restore_check = commands.add_parser('restore-check', add_help=False, help='проверить целостность нового плана восстановления')
    _help(restore_check)
    restore_check.add_argument('plan', type=Path, help='сохранённый новый план')
    restore_check.set_defaults(action='restore-check')
    restore_apply = commands.add_parser('restore-apply', add_help=False, help='POST: выполнить отдельно разрешённый новый план восстановления')
    _help(restore_apply)
    restore_apply.add_argument('plan', type=Path, help='проверенный новый план')
    restore_apply.add_argument('--authorize-plan-sha256', required=True, help='точная SHA-256 нового плана после отдельного разрешения записи')
    _add_connection_arguments(restore_apply, delay=0.4)
    restore_apply.set_defaults(action='restore-apply')
    return parser

def _api(args, *, read_only: bool=False) -> UrbanApi:
    api = (ReadOnlyApi if read_only else UrbanApi)(args.base_url, delay=args.delay, timeout=args.timeout)
    direct = getattr(args, 'direct_server_ip', None)
    source = getattr(args, 'source_ip', None)
    if bool(direct) != bool(source):
        raise RuntimeError('Для прямого подключения нужны оба IP')
    if direct:
        api.configure_direct_address(direct, source)
    return api

def _distributed_package(args) -> tuple[dict, Path]:
    territory = get_territory(args.territory)
    if territory.distributed_workers != 4:
        raise RuntimeError('Комплект поддерживает ровно четыре worker')
    run_id = args.run or new_run_id()
    paths = run_paths(territory.key, run_id)
    if not paths.directory.exists():
        paths = create_run(territory.key, run_id)
    if not paths.audit.exists() or not paths.accepted.exists():
        prepare_territory(_api(args), territory, paths)
    master = create_distributed_master_plan(_api(args), territory, paths)
    runtime = args.runtime or ARTIFACTS / 'runtime' / 'urban-import'
    if args.runtime is None and (not (runtime / 'urban-import.exe').is_file()):
        build_worker_runtime(runtime)
    output = args.output or ROOT / 'distribution' / f'{territory.key}-{run_id}'
    package_distribution(paths, output, runtime)
    return (master, output)

def main() -> None:
    args = build_parser().parse_args()
    action = args.action
    if action == 'status':
        keys = list(read_json(RESULTS / 'index.json')['territories']) if args.territory == 'all' else [args.territory]
        for key in keys:
            entry, _ = load_result(key)
            print(json.dumps({'territory': key, **entry}, ensure_ascii=False, indent=2))
        return
    if action == 'restore-check':
        plan = read_restoration_plan(args.plan)
        print(json.dumps({'territory': plan['territory'], 'sha256': plan['sha256'], 'create': len(plan['create']), 'unresolved': plan['unresolved'], 'write_authorized': False}, ensure_ascii=False, indent=2))
        return
    if action == 'restore-apply':
        from .restoration import apply_restoration
        report = apply_restoration(_api(args), args.plan, args.authorize_plan_sha256)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if report['issues']:
            raise SystemExit(2)
        return
    if action == 'status-doc':
        expected = status_markdown()
        path = ROOT / 'docs' / 'status.md'
        if args.check:
            if path.read_text(encoding='utf-8') != expected:
                raise SystemExit('docs/status.md отличается от results/index.json')
        else:
            path.write_text(expected, encoding='utf-8', newline='\n')
        print('docs/status.md соответствует results/index.json')
        return
    if action in {'reproduce', 'verify-current', 'restore-preview'}:
        keys = list(read_json(RESULTS / 'index.json')['territories']) if args.territory == 'all' else [args.territory]
        reports = []
        for key in keys:
            print(f'Начало {action}: {key}', flush=True)
            if action == 'reproduce':
                report, _ = reproduce(key)
            elif action == 'verify-current':
                report = verify_current(_api(args, read_only=True), key, check_old_geometries=args.check_old_geometries)
            else:
                output = args.output or ARTIFACTS / 'restoration' / new_run_id()
                if len(keys) > 1 or args.output is None:
                    output = output / f'{key}-plan.json'
                plan = create_restoration_plan(_api(args, read_only=True), key, output)
                report = {'territory': key, 'plan': str(output), 'create': len(plan['create']), 'preserve': len(plan['preserve']), 'issues': plan['unresolved'], 'write_authorized': False}
            reports.append(report)
            print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
        if any((report.get('issues') for report in reports)):
            raise SystemExit(2)
        return
    if action == 'verify' and args.run is None and (RESULTS / 'index.json').exists():
        result = verify_current(_api(args, read_only=True), args.territory)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if result['issues']:
            raise SystemExit(2)
        return
    if action == 'worker-status':
        result, location = (worker_status(args.bundle), Path(args.bundle))
    elif action == 'worker-stop':
        result, location = (request_worker_stop(args.bundle), Path(args.bundle))
    elif action == 'worker-export':
        archive, complete = export_worker_result(args.bundle)
        result, location = ({'archive': str(archive), 'complete': complete}, archive)
        if not complete:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit(4)
    elif action == 'worker-run':
        result = worker_run(args.bundle, _api(args), dry_run=args.dry_run, wait=not args.once)
        location = Path(args.bundle)
        if result.get('stopped') or (args.once and result.get('waiting')):
            print(json.dumps(result, ensure_ascii=False, indent=2))
            raise SystemExit(3)
    elif action == 'collect-results':
        result = collect_worker_results(args.distribution, args.results)
        location = Path(args.distribution) / 'collected.json'
    elif action == 'verify-final':
        result = final_verify(_api(args), args.distribution)
        location = Path(args.distribution) / 'final-verification.json'
    elif action == 'distributed-authorize':
        paths = create_authorization(args.distribution, replacement_confirmed=args.confirm_complete_replacement, destructive_apply_authorized=args.confirm_destructive_apply)
        result = {'authorization_files': [str(path) for path in paths]}
        location = Path(args.distribution)
    elif action == 'distributed-package':
        master, location = _distributed_package(args)
        result = {'run_id': master['run_id'], 'master_plan_sha256': master['sha256'], 'counts': master['counts'], 'unresolved': master['unresolved'], 'destructive_apply_authorized': False}
    else:
        territory = get_territory(args.territory)
        api = _api(args)
        if action == 'prepare':
            paths = create_run(territory.key, args.run)
            if not args.live_boundary and (RESULTS / 'index.json').exists() and (territory.key in read_json(RESULTS / 'index.json')['territories']):
                result = prepare_completed_result(territory.key, paths)
            else:
                result = prepare_territory(api, territory, paths)
        else:
            paths = resolve_run(territory.key, args.run)
            if territory.distributed_workers > 1 and action in {'preview', 'apply'}:
                raise RuntimeError('Для территории разрешён только distributed-package; apply заблокирован')
            if action == 'preview':
                plan = create_plan(api, territory, paths)
                result = {'planned_delete': len(plan['existing_ids']), 'retained': len(plan['retained_ids']), 'planned_create': plan['counts']['accepted'], 'unresolved': plan['unresolved']}
            elif action == 'apply':
                result = apply_plan(api, territory, paths)
            else:
                result = verify_run(api, territory, paths)
        location = paths.directory
    print(f'LOCATION: {location}')
    print(json.dumps(result, ensure_ascii=False, indent=2))
if __name__ == '__main__':
    main()
