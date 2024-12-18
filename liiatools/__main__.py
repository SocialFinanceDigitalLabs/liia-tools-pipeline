import click as click

from liiatools.annex_a_pipeline.cli import annex_a
from liiatools.cin_census_pipeline.cli import cin_census
from liiatools.ssda903_pipeline.cli import s903


@click.group()
def cli():
    pass


cli.add_command(annex_a)
cli.add_command(cin_census)
cli.add_command(s903)

if __name__ == "__main__":
    cli()
