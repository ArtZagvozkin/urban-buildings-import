"""Единая командная строка обычного и распределённого импорта."""
import argparse
import json
from pathlib import Path
from .api import DEFAULT_BASE_URL, UrbanApi
from .config import ROOT, get_territory
from .prepare import prepare_territory
from .storage import ARTIFACTS, create_run, new_run_id, resolve_run, run_paths
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
    for name, description in [('prepare', 'проверить GeoJSON без изменения API')]:
        command = commands.add_parser(name, parents=[common], add_help=False, help=description, description=description)
        _help(command)
        command.set_defaults(action=name)
        if name == 'prepare':
            command.add_argument('--live-boundary', action='store_true', help='для нового импорта получить новую границу API')
    return parser

def _api(args, *, read_only: bool=False) -> UrbanApi:
    api = UrbanApi(args.base_url, delay=args.delay, timeout=args.timeout)
    direct = getattr(args, 'direct_server_ip', None)
    source = getattr(args, 'source_ip', None)
    if bool(direct) != bool(source):
        raise RuntimeError('Для прямого подключения нужны оба IP')
    if direct:
        api.configure_direct_address(direct, source)
    return api

def main() -> None:
    args = build_parser().parse_args()
    action = args.action
    territory = get_territory(args.territory)
    api = _api(args)
    if action == 'prepare':
        paths = create_run(territory.key, args.run)
        result = prepare_territory(api, territory, paths)
    location = paths.directory
    print(f'LOCATION: {location}')
    print(json.dumps(result, ensure_ascii=False, indent=2))
if __name__ == '__main__':
    main()
