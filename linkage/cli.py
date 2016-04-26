import click
import yaml
import logging

from linkage.model import Linkage


@click.command()
@click.argument('config', type=click.File('rb'))
def cli(config):
    logging.basicConfig(level=logging.DEBUG)
    linkage = Linkage(yaml.load(config))

    for view in linkage.views:
        if not view.check_linktab():
            view.generate_linktab()

    for crossref in linkage.crossrefs:
        for ref in crossref.results():
            print ref
