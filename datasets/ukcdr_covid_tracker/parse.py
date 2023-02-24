import re
from typing import Any

import pandas as pd
import requests
from fingerprints import generate as fp
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URL = "https://www.ukcdr.org.uk/covid-circle/covid-19-research-project-tracker/"
FILE_URL = r".*(\/wp-content\/uploads/\d{4}\/\d{2}\/COVID-19-Research-Project-Tracker.*\.xlsx).*"


def clean(value: Any) -> str | None:
    if pd.isna(value):
        return None
    test_value = str(value).strip().lower()
    if not test_value or test_value == "unknown":
        return None
    return value


def clean_amount(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return str(round(pd.to_numeric(value), 2))
    except ValueError:
        return value


def clean_institution_name(value: str | None) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    name, *country = value.split(",")
    if len(country) == 1:
        return name, country
    m = re.match(r"(?P<name>.*)\s\((?P<country>[\w]{2})\).*", value)
    if m is not None:
        return m.groups()
    return name, None


def strip_slug(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) > 245:
        slug = value[:245]
        return f"{slug}-{make_entity_id(slug)[:10]}"
    return value


def pick_name(value: Any, ix: int) -> str | None:
    if pd.isna(value):
        return None
    names = str(value).split(",")
    names = [n.strip() for n in names]
    if len(names) > ix:
        return names[ix]


def make_project(context: Zavod, row: dict[str, Any]) -> CE:
    proxy = context.make("Project")
    ident = row.pop("Funder Project ID/Reference Number")
    who_ident = row.pop("Unique database reference number")
    proxy.id = context.make_slug("project", ident or who_ident)
    proxy.add("name", row.pop("Project Title"))
    proxy.add("projectId", ident)
    proxy.add("projectId", who_ident)
    proxy.add("keywords", row.pop("PRIMARY WHO Research Priority Area Name(s)"))
    proxy.add("keywords", row.pop("SECONDARY WHO Research Priority Area Name(s)"))
    proxy.add("keywords", row.pop("Study Population"))
    proxy.add("amount", clean_amount(row.pop("Amount Awarded")))
    proxy.add("currency", row.pop("Currency"))
    proxy.add("amountUsd", clean_amount(row.pop("Amount Awarded converted to USD")))
    for country in str(
        row.pop("Country/ countries research is being are conducted")
    ).split(","):
        country = country.strip()
        proxy.add("country", country)
    proxy.add("startDate", row.pop("Start Date"))
    proxy.add("endDate", row.pop("End Date"))
    proxy.add("summary", row.pop("Abstract"))
    proxy.add("summary", row.pop("Lay Summary"))
    proxy.add("notes", row.pop("Notes"))
    return proxy


def make_institution(context: Zavod, name: str, row: dict[str, Any]) -> CE:
    proxy = context.make("Organization")
    name, country = clean_institution_name(name)
    proxy.id = strip_slug(context.make_slug("org", fp(name)))
    proxy.add("name", name)
    proxy.add("country", country)
    return proxy


def make_person(
    context: Zavod, ix: int, name: str, ident: str, row: dict[str, Any]
) -> CE | None:
    proxy = context.make("Person")
    proxy.id = strip_slug(context.make_slug("investigator", ident, fp(name)))
    if proxy.id is None:
        return None
    proxy.add("name", name)
    proxy.add("firstName", pick_name(row["PI First Name"], ix))
    proxy.add("lastName", pick_name(row["PI Last Name"], ix))
    proxy.add("title", pick_name(row["PI Title"], ix))
    return proxy


def make_rel(context: Zavod, project: CE, participant: CE) -> CE:
    rel = context.make("ProjectParticipant")
    rel.id = strip_slug(context.make_slug("participation", participant.id, project.id))
    rel.add("project", project)
    rel.add("participant", participant)
    rel.add("startDate", project.get("startDate"))
    rel.add("endDate", project.get("endDate"))
    return rel


def parse_row(context: Zavod, row: dict[str, Any]):
    project = make_project(context, row)
    context.emit(project)
    institution = row.pop("Lead Institution")
    person_ident = fp(sorted(project.get("projectId"))[0])
    if fp(institution) is not None:
        institution = make_institution(context, institution, row)
        rel = make_rel(context, project, institution)
        rel.add("role", "LEAD INSTITUTION")
        context.emit(institution)
        context.emit(rel)
        person_ident = institution.id
    person = row.pop("Principal Investigator (PI)")
    if fp(person) is not None:
        sep = ","
        if ";" in person:
            sep = ";"
        for ix, name in enumerate(person.split(sep)):
            person = make_person(context, ix, name, person_ident, row)
            if person is not None:
                rel = make_rel(context, project, person)
                rel.add("role", "PRINCIPAL INVESTIGATOR")
                context.emit(person)
                context.emit(rel)


def parse(context: Zavod):
    res = requests.get(URL)
    url = re.search(FILE_URL, res.text).groups()[0]
    url = "https://www.ukcdr.org.uk" + url
    data_path = context.fetch_resource("ukcdr_projects.xlsx", url)
    df = pd.read_excel(data_path, "Funded Research Projects")
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    df = df.applymap(clean)
    ix = 0
    for ix, row in df.iterrows():
        parse_row(context, row)
        if ix and ix % 1_000 == 0:
            context.log.info("Parse row %d ..." % ix)
    if ix:
        context.log.info("Parsed %d rows." % (ix + 1), url=URL)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
