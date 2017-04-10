import sys
import argparse

from .index import CLI as indexcli
from .query import CLI as querycli
from .query import Index

subcommands = [indexcli, querycli]

Index  # pyflakes


def cli():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    for c in subcommands:
        sp = subparsers.add_parser(c.name, help=getattr(c, "help", None))
        for args, kwargs in c.arguments:
            sp.add_argument(*args, **kwargs)
        sp.set_defaults(func=c.hook)
    args = parser.parse_args()
    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit()
    return args.func(parser, args)


if __name__ == "__main__":
    cli()
