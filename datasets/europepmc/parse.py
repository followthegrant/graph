import csv
import gzip
import io
from typing import Generator

from followthegrant.model import Article
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URL = "https://europepmc.org/pub/databases/pmc/DOI/PMID_PMCID_DOI.csv.gz"
Row = tuple[str, str, str]


def make_article(context: Zavod, row: Row) -> CE:
    pmid, pmc, doi = row
    article = Article(pmid=pmid, pmc=pmc, doi=doi)
    context.emit(article.proxy)


def stream_csv(stream: csv.reader) -> Generator[Row, None, None]:
    # omit header: PMID,PMCID,DOI
    next(stream)
    yield from stream


def parse(context: Zavod):
    data_path = context.fetch_resource("PMID_PMCID_DOI.csv.gz", URL)
    with gzip.open(data_path) as zf:
        with io.TextIOWrapper(zf) as f:
            ix = 0
            reader = csv.reader(f)
            for ix, row in enumerate(stream_csv(reader)):
                make_article(context, row)
                if ix and ix % 10_000 == 0:
                    context.log.info("Parse row %d ..." % ix)
            if ix:
                context.log.info("Parsed %d rows." % (ix + 1), fp=data_path)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
