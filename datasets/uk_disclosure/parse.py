from typing import Any
from zipfile import ZipFile

import pandas as pd
from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from ftm_geocode.util import get_country_code
from nomenklatura.entity import CE
from zavod import Zavod, init_context
from zavod.parse.addresses import make_address as zavod_make_address


def make_address(context: Zavod, data: dict[str, Any], country: str) -> CE:
    proxy = zavod_make_address(
        context,
        remarks=data.pop("Location"),
        street=data.pop("Address Line 1"),
        street2=data.pop("Address Line 2"),
        city=data.pop("City", None),
        postal_code=data.pop("Postcode"),
        country=country,
        country_code=get_country_code(country),
    )

    context.emit(proxy)
    return proxy


def make_organization(
    context: Zavod, data: dict[str, Any], with_address: bool | None = True
) -> CE:
    country = data.pop("Country")

    proxy = context.make("Organization")
    proxy.add("name", data.pop("Institution Name"))
    proxy.add("country", country)
    proxy.id = context.make_slug("hco", fp(proxy.caption))

    if with_address:
        address = make_address(context, data, country)
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)

    if proxy.id:  # hcp orgs are empty
        context.emit(proxy)
    return proxy


def make_person(context: Zavod, data: dict[str, Any]) -> CE:
    institution = make_organization(context, data, with_address=False)

    proxy = context.make("Person")
    title, first, middle, last = (
        data.pop("Title"),
        data.pop("First Name"),
        data.pop("Initial"),
        data.pop("Last Name"),
    )
    proxy.add("name", join_text(title, first, middle, last))
    proxy.add("title", title)
    proxy.add("firstName", first)
    proxy.add("middleName", middle)
    proxy.add("lastName", last)
    proxy.add("country", institution.get("country"))
    proxy.add("description", data.pop("Speciality"))
    proxy.id = context.make_slug(
        "hcp", make_entity_id(fp(proxy.caption), institution.id)
    )
    country = institution.first("country")
    address = make_address(context, data, country)
    proxy.add("address", address.caption)
    proxy.add("addressEntity", address)
    context.emit(proxy)

    if institution.id:
        rel = context.make("Membership")
        rel.add("organization", institution)
        rel.add("member", proxy)
        rel.add("role", data.pop("Role"))
        rel.id = context.make_slug(
            "membership", make_entity_id(institution.id, proxy.id)
        )
        context.emit(rel)

    return proxy


def make_company(context: Zavod, data: dict[str, Any]) -> CE:
    proxy = context.make("Company")
    proxy.add("name", data.pop("Pharma Company Name"))
    proxy.add("country", "gb")
    proxy.id = context.make_slug("company", fp(proxy.caption))

    context.emit(proxy)
    return proxy


def make_payment(context: Zavod, payer: CE, beneficiary: CE, **data) -> CE:
    payment = context.make("Payment")
    payment.add("payer", payer)
    payment.add("beneficiary", beneficiary)
    payment.add("amount", data["amount"])
    payment.add("currency", "GBP")
    payment.add("date", data["year"])
    payment.add("purpose", data["purpose"])
    payment.add(
        "programme", "Disclosure UK – Payments from the pharmaceutical industry"
    )
    payment.add("sourceUrl", data["source_url"])
    payment.id = context.make_slug(
        "payment",
        make_entity_id(
            payer, beneficiary, data["year"], data["purpose"], data["amount"]
        ),
    )
    return payment


def make_payments(context: Zavod, payer: CE, beneficiary: CE, data: dict[str, Any]):
    source_url = data.pop(
        "Collaborative Working link", data.pop("Joint Working Link", None)
    )
    year = data.pop("Year of Disclosure")
    for col, amount in data.items():
        if "Unnamed" not in col:
            try:
                if amount and int(amount) > 0:
                    payment = make_payment(
                        context,
                        payer,
                        beneficiary,
                        year=year,
                        amount=amount,
                        purpose=col,
                        source_url=source_url,
                    )
                    context.emit(payment)
            except ValueError:
                pass


def parse_hco(context: Zavod, data: dict[str, Any]):
    payer = make_company(context, data)
    beneficiary = make_organization(context, data)
    make_payments(context, payer, beneficiary, data)


def parse_hcp(context: Zavod, data: dict[str, Any]):
    payer = make_company(context, data)
    beneficiary = make_person(context, data)
    make_payments(context, payer, beneficiary, data)


def parse(context: Zavod):
    data_src = context.get_resource_path("src")
    for data_path in data_src.glob("*.zip"):
        with ZipFile(data_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith("xlsx"):
                    context.log.info("Opening: %s in %s" % (name, data_path))
                    with zf.open(name) as f:
                        ix = 0
                        df = pd.read_excel(f, sheet_name="HCO", skiprows=1).fillna("")
                        for ix, row in df.iterrows():
                            parse_hco(context, dict(row))
                            if ix and ix % 10_000 == 0:
                                context.log.info("Parse HCO record %d ..." % ix)
                        if ix:
                            context.log.info(
                                "Parsed %d HCO records." % (ix + 1), fp=name
                            )
                        ix = 0
                        df = pd.read_excel(f, sheet_name="HCP", skiprows=1).fillna("")
                        for ix, row in df.iterrows():
                            parse_hcp(context, dict(row))
                            if ix and ix % 10_000 == 0:
                                context.log.info("Parse HCP record %d ..." % ix)
                        if ix:
                            context.log.info(
                                "Parsed %d HCP records." % (ix + 1), fp=name
                            )


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
