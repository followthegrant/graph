from typing import Any
from zipfile import ZipFile

import pandas as pd
from fingerprints import generate as fp
from ftm_geocode.util import get_country_code, get_country_name
from nomenklatura.entity import CE
from zavod import Zavod, init_context
from zavod.parse.addresses import make_address


def make_payer(context: Zavod, data: dict[str, Any], country: str) -> CE:
    proxy = context.make("Organization")
    proxy.id = context.make_slug(data.pop("clean_source_organization_id"))
    proxy.add("name", data.pop("source_organisation_full_name"))
    proxy.add("country", country)

    context.emit(proxy)
    return proxy


def make_beneficiary(context: Zavod, data: dict[str, Any], country: str) -> CE | None:
    ident = data.get("recipient_entity_id", data.get("recipient_id"))
    if ident:
        if data.pop("recipient_entity_is_person"):
            proxy = context.make("Person")
        else:
            proxy = context.make("Organization")

        proxy.id = context.make_slug(ident)
        proxy.add(
            "name",
            data.get("recipient_entity_full_name", data.get("recipient_full_name")),
        )
        proxy.add("legalForm", data.pop("recipient_entity_type"))
        proxy.add("country", country)

        city = data.get("recipient_entity_city", data.get("recipient_city"))
        if fp(city):
            address = make_address(
                context,
                city=city,
                country=get_country_name(country),
                country_code=get_country_code(country),
            )

            if address.id:
                context.emit(address)
                proxy.add("address", address.caption)
                proxy.add("addressEntity", address)

        context.emit(proxy)
        return proxy


def make_payment(
    context: Zavod, payer: CE, beneficiary: CE | None, data: dict[str, Any]
) -> CE:
    payment = context.make("Payment")
    payment.id = context.make_slug("rel", data.pop("link_id"))
    payment.add("payer", payer)
    payment.add("beneficiary", beneficiary)
    payment.add("date", data.pop("year"))
    payment.add("purpose", data.pop("type"))
    payment.add("purpose", data.pop("category"))
    payment.add("amount", data.pop("value_total_amount"))
    payment.add("currency", data.pop("currency"))
    payment.add("amountEur", data.pop("value_total_amount_eur"))
    payment.add("programme", "eurosfordocs – Payments from the pharmaceutical industry")
    payment.add("sourceUrl", data.pop("publication_url"))

    context.emit(payment)


def parse_row(context: Zavod, data: dict[str, Any]):
    country = get_country_code(data.get("publication_country", data.get("country")))
    if country is None:
        context.log.warning("Invalid country!", id=data["link_id"])
        return

    payer = make_payer(context, data, country)
    beneficiary = make_beneficiary(context, data, country)
    make_payment(context, payer, beneficiary, data)


def parse(context: Zavod):
    data_src = context.get_resource_path("src")
    for data_path in data_src.glob("*.zip"):
        with ZipFile(data_path, "r") as zf:
            for name in zf.namelist():
                if not name.startswith("__MACOSX") and name.endswith("csv"):
                    context.log.info("Opening: %s in %s" % (name, data_path))
                    with zf.open(name) as f:
                        ix = 0
                        df = pd.read_csv(f, dtype=str).fillna("")
                        for ix, row in df.iterrows():
                            parse_row(context, dict(row))
                            if ix and ix % 10_000 == 0:
                                context.log.info("Parse record %d ..." % ix)
                        if ix:
                            context.log.info("Parsed %d records." % (ix + 1), fp=name)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
