import csv
import gzip
import io
from typing import Generator

from followthegrant.model import ParsedResult
from followthegrant.transform import make_proxies
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz"
Row = dict[str, str]


def make_article(context: Zavod, row: Row) -> CE:
    result = {
        "journal": {
            "name": row.pop("Journal Title"),
            "issn": [row.pop("ISSN"), row.pop("eISSN")],
        },
        "article": {
            "date": row.pop("Year"),
            "doi": row.pop("DOI"),
            "pmc": row.pop("PMCID"),
            "pmid": row.pop("PMID"),
        },
    }
    result = ParsedResult(**result)
    for proxy in make_proxies(result):
        context.emit(proxy)


def stream_csv(stream: csv.reader) -> Generator[Row, None, None]:
    # omit header: PMID,PMCID,DOI
    header = next(stream)
    for row in stream:
        yield dict(zip(header, row))


def parse(context: Zavod):
    data_path = context.fetch_resource("PMC-ids.csv.gz", URL)
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
