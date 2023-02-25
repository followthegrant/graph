import csv
import io
import sys
from typing import Any, Generator, Literal
from zipfile import ZipFile

from dateparser import parse as parse_date
from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from ftm_geocode.util import get_country_code
from nomenklatura.entity import CE
from zavod import Zavod, init_context
from zavod.parse.addresses import make_address

Data = dict[str, Any]

COLUMNS = {
    "Recipient_Primary_Business_Street_Address_Line1": "Recipient_Address_Line_1",
    "Recipient_Primary_Business_Street_Address_Line2": "Recipient_Address_Line_2",
    "Covered_Recipient_Profile_Primary_Specialty": "Recipient_Specialty",
    "Covered_Recipient_Profile_Country_Name": "Recipient_Country",
    "Covered_Recipient_Profile": "Recipient",
    "Physician_Profile": "Recipient",
    "Covered_Recipient": "Recipient",
    "Physician": "Recipient",
}

DESCRIPTION = (
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "Product_Category_or_Therapeutic_Area_1",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "Associated_Drug_or_Biological_NDC_1",
    "Covered_or_Noncovered_Indicator_2",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2",
    "Product_Category_or_Therapeutic_Area_2",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
    "Associated_Drug_or_Biological_NDC_2",
    "Covered_or_Noncovered_Indicator_3",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3",
    "Product_Category_or_Therapeutic_Area_3",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
    "Associated_Drug_or_Biological_NDC_3",
    "Covered_or_Noncovered_Indicator_4",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4",
    "Product_Category_or_Therapeutic_Area_4",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
    "Associated_Drug_or_Biological_NDC_4",
    "Covered_or_Noncovered_Indicator_5",
    "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5",
    "Product_Category_or_Therapeutic_Area_5",
    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
    "Associated_Drug_or_Biological_NDC_5",
    "Expenditure_Category1",
    "Expenditure_Category2",
    "Expenditure_Category3",
    "Expenditure_Category4",
    "Expenditure_Category5",
    "Expenditure_Category6",
)


def get_description(data: Data) -> str:
    parts = (data.get(k) for k in DESCRIPTION)
    return join_text(*parts, sep="\n\n")


def make_recipient_address(context: Zavod, data: Data) -> CE | None:
    country = data.pop("Recipient_Country")
    postal_code = data.pop(
        "Recipient_Zipcode",
        data.pop("Recipient_Zip_Code", data.pop("Recipient_Postal_Code", None)),
    )
    parts = {
        "street": data.pop(
            "Recipient_Address_Line_1", data.pop("Recipient_Address_Line1", None)
        ),
        "street2": data.pop(
            "Recipient_Address_Line_2", data.pop("Recipient_Address_Line2", None)
        ),
        "postal_code": postal_code,
        "city": data.pop("Recipient_City"),
        "region": data.pop(
            "Recipient_Province", data.pop("Recipient_Province_Name", None)
        ),
        "state": data.pop("Recipient_State"),
        "country": country,
        "country_code": get_country_code(country),
    }
    proxy = make_address(context, **parts)

    if proxy.id:
        context.emit(proxy)
        return proxy


def make_recipient_person(context: Zavod, data: Data) -> CE | None:
    proxy = context.make("Person")
    proxy.id = context.make_slug("physician", data.pop("Recipient_ID"))
    if proxy.id is None:
        return

    first, middle, last = (
        data.pop("Recipient_First_Name"),
        data.pop("Recipient_Middle_Name"),
        data.pop("Recipient_Last_Name"),
    )
    name = join_text(first, middle, last)
    proxy.add("name", name)
    proxy.add("firstName", first)
    proxy.add("firstName", data.pop("Recipient_Alternate_First_Name", None))
    proxy.add("middleName", middle)
    proxy.add("middleName", data.pop("Recipient_Alternate_Middle_Name", None))
    proxy.add("lastName", last)
    proxy.add("lastName", data.pop("Recipient_Alternate_Last_Name", None))
    summary = data.pop("Recipient_Specialty", None)
    if summary is not None:
        proxy.add("summary", summary)
        proxy.add("keywords", summary.split("|"))

    address = make_recipient_address(context, data)
    if address is not None:
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)
        proxy.add("country", address.countries)

    context.emit(proxy)
    return proxy


def make_recipient_org(context: Zavod, data: Data) -> CE | None:
    proxy = context.make("Organization")
    proxy.id = context.make_slug("org", data.pop("Teaching_Hospital_ID"))
    if proxy.id is None:
        return

    proxy.add("name", data.pop("Teaching_Hospital_Name"))
    address = make_recipient_address(context, data)
    proxy.add("address", address.caption)
    proxy.add("addressEntity", address)
    proxy.add("country", address.countries)

    context.emit(proxy)
    return proxy


def make_unknown_recipient(context: Zavod, data: Data) -> CE | None:
    proxy = context.make("LegalEntity")
    name = data.pop("Noncovered_Recipient_Entity_Name")
    if not fp(name):
        return

    address = make_recipient_address(context, data)
    ident = address.id or data["Program_Year"]
    proxy.id = context.make_slug("entity", make_entity_id(fp(name), ident))
    proxy.add("name", name)

    context.emit(proxy)
    return proxy


def make_recipient(context: Zavod, data: Data) -> CE | None:
    type_ = data.pop("Recipient_Type")
    if type_ == "Covered Recipient Physician":
        return make_recipient_person(context, data)
    if type_ == "Covered Recipient Non-Physician Practitioner":
        return make_recipient_person(context, data)
    if type_ == "Covered Recipient Teaching Hospital":
        return make_recipient_org(context, data)
    if "Non-covered Recipient" in type_:
        return make_unknown_recipient(context, data)

    context.log.warning(f"Unknown recipient type: `{type_}`")


def make_company(context: Zavod, data: Data) -> CE | None:
    proxy = context.make("Company")
    proxy.id = context.make_slug(
        "org", data.pop("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID")
    )
    if proxy.id is None:
        return

    proxy.add(
        "name",
        data.pop("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"),
    )
    proxy.add(
        "country",
        data.pop("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"),
    )

    context.emit(proxy)
    return proxy


def connect_physicians(context: Zavod, physician: CE, other_id: str):
    if not other_id:
        return

    proxy = context.make("UnknownLink")
    proxy.id = context.make_slug("similar", make_entity_id(physician.id, other_id))
    proxy.add("subject", physician)
    proxy.add("object", other_id)
    proxy.add("role", "same as")
    proxy.add("summary", "different profile associated with the same physician")

    context.emit(proxy)


def parse_physician(context: Zavod, data: Data):
    proxy = make_recipient_person(context, data)
    if proxy is not None:
        connect_physicians(
            context, proxy, data.pop("Associated_Covered_Recipient_Profile_ID_1")
        )
        connect_physicians(
            context, proxy, data.pop("Associated_Covered_Recipient_Profile_ID_2")
        )


def parse_ownership(context: Zavod, data: Data):
    asset = make_company(context, data)
    owner = make_recipient_person(context, data)

    if asset.id and owner.id:
        ident = data.pop("Record_ID")
        proxy = context.make("Ownership")
        proxy.id = context.make_slug(
            "ownership", make_entity_id(owner.id, asset.id, ident)
        )
        proxy.add("date", data.pop("Program_Year"))
        proxy.add("ownershipType", data.pop("Terms_of_Interest"))
        proxy.add(
            "role", data.pop("Interest_Held_by_Physician_or_an_Immediate_Family_Member")
        )
        proxy.add("sharesValue", data.pop("Total_Amount_Invested_USDollars"))
        proxy.add("sharesValue", data.pop("Value_of_Interest"))
        proxy.add("sharesCurrency", "USD")
        proxy.add("recordId", ident)

        context.emit(owner)
        context.emit(asset)
        context.emit(proxy)


def make_participation(
    context: Zavod,
    project: CE | None,
    participant: CE | None,
    data: Data,
    role: str | None = None,
):
    if project is None or participant is None:
        return

    proxy = context.make("ProjectParticipant")
    proxy.id = context.make_slug(
        "project-participant", make_entity_id(project.id, participant.id)
    )
    proxy.add("project", project)
    proxy.add("participant", participant)
    proxy.add("role", role)
    proxy.add("date", data["Program_Year"])
    proxy.add("sourceUrl", data["Research_Information_Link"])

    context.emit(proxy)


def make_payment(
    context: Zavod,
    data: Data,
    payer: CE | None = None,
    beneficiary: CE | None = None,
    project: CE | None = None,
):
    idents = [data["Record_ID"]]
    if payer:
        idents.append(payer.id)
    if beneficiary:
        idents.append(beneficiary.id)
    if project:
        idents.append(project.id)
    proxy = context.make("Payment")
    proxy.id = context.make_slug("payment", make_entity_id(*idents))
    proxy.add("payer", payer)
    proxy.add("beneficiary", beneficiary)
    proxy.add("project", project)
    proxy.add("recordId", idents[0])
    amount = data.pop("Total_Amount_of_Payment_USDollars")
    proxy.add("amount", amount)
    proxy.add("amountUsd", amount)
    proxy.add("currency", "USD")
    proxy.add("date", parse_date(data.pop("Date_of_Payment")))
    proxy.add("purpose", data.pop("Form_of_Payment_or_Transfer_of_Value"))
    proxy.add("programme", data.get("Nature_of_Payment_or_Transfer_of_Value"))
    proxy.add("summary", data.get("Contextual_Information"))
    proxy.add("description", get_description(data))

    context.emit(proxy)


def parse_research(context: Zavod, data: Data):
    project = None
    projectName = data.pop("Name_of_Study")
    if fp(projectName):
        project = context.make("Project")
        projectId = data.pop("ClinicalTrials_Gov_Identifier")
        project.id = context.make_slug("project", projectId) or context.make_slug(
            "project", make_entity_id(fp(projectName))
        )
        project.add("name", projectName)
        project.add("projectId", projectId)
        project.add("date", data["Program_Year"])
        project.add("sourceUrl", data["Research_Information_Link"])
        project.add("notes", data.pop("Context_of_Research"))
        project.add("description", get_description(data))

    recipient = make_recipient(context, data)
    if recipient is not None:
        if project is not None:
            project.add("country", recipient.countries)

        try:
            for i in range(1, 6):
                recipient.add("summary", data.pop(f"Recipient_Primary_Type_{i}"))
                recipient.add("summary", data.pop(f"Recipient_Specialty_{i}"))
        except KeyError:
            recipient.add("summary", data.pop("Recipient_Primary_Type"))

    for i in range(1, 6):
        investigator_data = {
            k.replace(f"Principal_Investigator_{i}", "Recipient"): v
            for k, v in data.items()
            if k.startswith(f"Principal_Investigator_{i}")
        }
        investigator_data["Recipient_ID"] = investigator_data.pop(
            "Recipient_Profile_ID"
        )
        investigator = make_recipient_person(context, investigator_data)
        if investigator is not None:
            try:
                for j in range(1, 6):
                    investigator.add(
                        "summary",
                        data.pop(f"Principal_Investigator_{i}_Primary_Type_{j}"),
                    )
                    investigator.add(
                        "summary", data.pop(f"Principal_Investigator_{i}_Specialty_{j}")
                    )
            except KeyError:
                investigator.add(
                    "summary",
                    data.pop(f"Principal_Investigator_{i}_Primary_Type"),
                )
                investigator.add(
                    "summary", data.pop(f"Principal_Investigator_{i}_Specialty")
                )
            if project is not None:
                project.add("country", investigator.countries)
            make_participation(
                context, project, investigator, data, "Principal investigator"
            )

    company = make_company(context, data)

    make_participation(context, project, company, data, "Financier")
    make_participation(context, project, recipient, data)
    make_payment(context, data, payer=company, beneficiary=recipient, project=project)

    if project is not None:
        context.emit(project)


def parse_general(context: Zavod, data: Data):
    company = make_company(context, data)
    recipient = make_recipient(context, data)
    make_payment(context, data, payer=company, beneficiary=recipient)


HANDLERS = {
    "PRFL_SPLMTL": parse_physician,
    "OWNRSHP": parse_ownership,
    "RSRCH": parse_research,
    "GNRL": parse_general,
}


def get_handler(fname: str) -> Literal[parse_physician] | None:
    for key, handler in HANDLERS.items():
        if key in fname:
            return handler


def stream_csv(stream: csv.reader) -> Generator[Data, None, None]:
    header = next(stream)
    columns = []
    for c in header:
        seen = False
        for prefix, key in COLUMNS.items():
            if c.startswith(prefix):
                columns.append(c.replace(prefix, key))
                seen = True
                break
        if not seen:
            columns.append(c)

    for row in stream:
        yield dict(zip(columns, row))


def parse(context: Zavod, prefix: str | None = None):
    data_src = context.get_resource_path("src")
    for data_path in data_src.glob("*.ZIP"):
        if prefix is not None and not data_path.name.startswith(prefix):
            continue

        with ZipFile(data_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith("csv"):
                    context.log.info("Opening: %s in %s" % (name, data_path))
                    handler = get_handler(name)
                    if handler is None:
                        context.log.warning(f"No handler for file `{name}`")
                        continue

                    with zf.open(name) as fh:
                        with io.TextIOWrapper(fh) as f:
                            ix = 0
                            reader = csv.reader(f)
                            for ix, row in enumerate(stream_csv(reader)):
                                handler(context, row)
                                if ix and ix % 10_000 == 0:
                                    context.log.info("Parse record %d ..." % ix)
                            if ix:
                                context.log.info(
                                    "Parsed %d records." % (ix + 1), fp=name
                                )


if __name__ == "__main__":
    with init_context("metadata.yml", sink_type="ftmstore") as context:
        prefix = None
        if len(sys.argv) > 1:
            prefix = sys.argv[1]
        context.export_metadata("export/index.json")
        parse(context, prefix)
