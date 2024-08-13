import click as click

from liiatools.ssda903_pipeline.cli import s903
from liiatools.cin_census_pipeline.cli import cin_census


@click.group()
def cli():
    pass


cli.add_command(s903)
cli.add_command(cin_census)

if __name__ == "__main__":
    cli()
