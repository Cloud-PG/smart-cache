import argparse

from .simulator import get_simulator_exe, get_simulator_path
from .utils import str2bool


def main():
    parser = argparse.ArgumentParser(description='Go Simulator Utils')

    parser.register('type', 'bool', str2bool)  # add type keyword to registries

    parser.add_argument(
        'command', type=str, choices=["compile", "simPath"],
        help='the command to execute'
    )
    parser.add_argument(
        '--release', type='bool', default=False,
        help='build the release binary'
    )
    parser.add_argument(
        '--fast', type='bool', default=False,
        help='build only modified files'
    )

    args = parser.parse_args()
    if args.command == "compile":
        get_simulator_exe(True, args.release, args.fast)
    elif args.command == "simPath":
        _, simPath = get_simulator_path()
        print(simPath.as_posix())


if __name__ == "__main__":
    main()
