import datetime
import subprocess
import numpy as np
import pandas as pd
import sqlalchemy


PAST_MONTHS = 3


def create_tables(db_engine):
    # Create patient table
    db_engine.execute(('CREATE TABLE IF NOT EXISTS patient ("Pseudo_id" int PRIMARY KEY, "Geboortedatum" date, '
                       '"Geslacht" text, "Voornaam" text, "Achternaam" text, "Overlijdensdatum" date, '
                       '"IsOverleden" boolean)'))

    # Create prn table
    db_engine.execute('CREATE TABLE IF NOT EXISTS prn ("Totaal aantal dagen" int, p10 int, "Geslacht" text)')

    # Create partum table
    db_engine.execute(('CREATE TABLE IF NOT EXISTS partum ("Pseudo_id" int PRIMARY KEY, '
                       '"GeboorteGewichtInKilogram" NUMERIC, "GeboorteGewichtInGram" int, '
                       '"TermijnBijGeboorteInDagen" int, "TermijnBijGeboorteInWeken" int, '
                       '"IsOverleden" boolean, "OnderPercentiel" boolean)'))

    # Create appointment requests table
    db_engine.execute('CREATE TABLE IF NOT EXISTS app_req ("ZorgorderId" int PRIMARY KEY, "Pseudo_id" int, '
                      '"Orderopdracht" text)')

    # Create koppeltabel table
    db_engine.execute('CREATE TABLE IF NOT EXISTS koppeltabel ("Pseudo_id" int PRIMARY KEY, "Patientnummer" int)')

    # Create missing appointments table
    db_engine.execute('CREATE TABLE IF NOT EXISTS missing ("Pseudo_id" int PRIMARY KEY, "Patientnummer" int, '
                      '"Voornaam" text, "Achternaam" text, "Geboortedatum" date)')


def get_prn_data(path: str):
    data = []

    genders_child = ["meisjes", "jongens"]
    genders = ["Vrouw", "Man"]
    
    for gender_child, gender in zip(genders_child, genders):
        gender_data = (pd.read_excel(path, sheet_name=f"Tabel {gender_child}", header=2)
                         .drop(columns=[
                             "Unnamed: 10",
                             "Week",
                             "Dag",
                             "p3",
                             "p5",
                             "p50",
                             "p90",
                             "p95",
                             "p97",
                             "Week (exact)",
                             "p3.1",
                             "p5.1",
                             "p10.1",
                             "p50.1",
                             "p90.1",
                             "p95.1",
                             "p97.1"
                         ]))
        gender_data["Geslacht"] = gender
        
        data.append(gender_data.set_index("Totaal aantal dagen"))

    return pd.concat(data)

# Establish connection with database
db_name = "patient_selection"
patient_selection_engine = sqlalchemy.create_engine(f"postgresql://postgres:admin@localhost:5432/{db_name}")

# Create tables required for storing patient selection data
create_tables(patient_selection_engine)

# Get data from rdp
subprocess.call(r"\\amc.intra\users\O\opalafoxverna\home\R-Portable\App\R-Portable\bin\Rscript.exe --vanilla ./rdp.r",
                shell=True)

# Load patient data
patient = (pd.read_csv("data/patient.csv", parse_dates=["Geboortedatum", "Overlijdensdatum"])
             .drop(columns=["Unnamed: 0", "uitgifteDT"]))
patient["IsOverleden"] = ~patient["Overlijdensdatum"].isnull()
# Fetch saved data
existing_patients_ids = pd.DataFrame(patient_selection_engine.execute('SELECT "Pseudo_id" FROM patient').fetchall())["Pseudo_id"]
# Upload to SQL
patient[~patient["Pseudo_id"].isin(existing_patients_ids)].drop(columns=[
    "IsMeerling",
    "Volledigenaam",
    "Voorletters"
]).to_sql("patient", patient_selection_engine, if_exists="append", index=False)

# Load prn data
prn = get_prn_data("data/PRN-data.xlsx")
# Upload to SQL if empty prn table
if pd.DataFrame(patient_selection_engine.execute('SELECT * FROM prn').fetchall()).empty:
    prn.to_sql("prn", patient_selection_engine, if_exists="append", index=True)

# Load partum data
partum = (pd.read_csv("data/partum.csv", parse_dates=["PartusDatum"])
            .drop(columns=["Unnamed: 0", "uitgifteDT"]))
partum["TermijnBijGeboorteInWeken"] = np.floor(partum["TermijnBijGeboorteInDagen"] / 7).astype(pd.Int64Dtype())
# Track patients that cannot be automatically selected
undefinable = partum[partum[["TermijnBijGeboorteInDagen", "GeboorteGewichtInKilogram"]].isnull().any(axis=1)].reset_index(drop=True)
# Remove children without days or weight specified
partum = partum.dropna(subset=["TermijnBijGeboorteInDagen", "GeboorteGewichtInKilogram"])
# Compute start born date
start_born_date = datetime.datetime.utcnow() - datetime.timedelta(PAST_MONTHS * 31)
# Remove children born before 23 weeks and born before desired time range
partum = partum.drop(partum[(partum["TermijnBijGeboorteInWeken"] < 23) | (partum["PartusDatum"] < start_born_date)].index)
partum["GeboorteGewichtInGram"] = np.floor(partum["GeboorteGewichtInKilogram"] * 1000).astype(int)
partum["TermijnBijGeboorteInDagen"] = partum["TermijnBijGeboorteInDagen"].astype(int)
# Add sex data
partum = partum.merge(patient, on="Pseudo_id")
# Change days to be able to support percentile file
partum.loc[partum["TermijnBijGeboorteInDagen"] > 294, "TermijnBijGeboorteInDagen"] = 294
partum.loc[partum["TermijnBijGeboorteInDagen"] < 161, "TermijnBijGeboorteInDagen"] = 161
# Add whether patient is under 10th percentile
partum["OnderPercentiel"] = partum.apply(lambda x: ((prn.loc[prn["Geslacht"] == x["Geslacht"]]
                                                        .loc[x["TermijnBijGeboorteInDagen"], "p10"])
                                                    > x["GeboorteGewichtInGram"]),
                                         axis=1)
# Remove deceased patients
partum = partum[~partum["IsOverleden"]]
undefinable = undefinable[~undefinable["IsOverleden"]]
# Fetch saved data
existing_partums_ids = pd.DataFrame(patient_selection_engine.execute('SELECT "Pseudo_id" FROM partum').fetchall())["Pseudo_id"]
# Upload to SQL
partum[~partum["Pseudo_id"].isin(existing_partums_ids)].drop(columns=[
    "PartusId",
    "PartusDatum",
    "ApgarScoreNa1Minuut",
    "ApgarScoreNa5Minuten",
    "ApgarScoreNa10Minuten",
    "Geboortedatum",
    "Geslacht",
    "IsMeerling",
    "Volledigenaam",
    "Voornaam",
    "Voorletters",
    "Achternaam",
    "Overlijdensdatum"
]).to_sql("partum", patient_selection_engine, if_exists="append", index=False)

# Load patient requests
appointment_requests = (pd.read_csv("data/appointments_requests.csv", parse_dates=["AanvraagDatum", "IngeplandDatum"])
                          .drop(columns=["Unnamed: 0", "uitgifteDT"]))
# Fetch saved data
existing_app_reqs_ids = pd.DataFrame(patient_selection_engine.execute('SELECT "ZorgorderId" FROM app_req').fetchall())["ZorgorderId"]
# Upload to SQL
appointment_requests[~appointment_requests["ZorgorderId"].isin(existing_app_reqs_ids)].drop(columns=[
    "AanvraagDatum",
    "AfspraakAanvraagStatus",
    "OrderopdrachtCode",
    "OrderStatus",
    "OrderType",
    "OrderKlasse",
    "IngeplandDatum"
]).to_sql("app_req", patient_selection_engine, if_exists="append", index=False)

# Load patient number
koppeltabel = pd.read_csv("data/koppeltabel.csv").drop(columns=["Unnamed: 0", "CreatePseudoIdDT"])
# Fetch saved data
existing_koppeltabel_ids = pd.DataFrame(patient_selection_engine.execute('SELECT "Pseudo_id" FROM koppeltabel').fetchall())["Pseudo_id"]
# Upload to SQL
koppeltabel[~koppeltabel["Pseudo_id"].isin(existing_koppeltabel_ids)].to_sql("koppeltabel", patient_selection_engine, if_exists="append", index=False)


# Select patients
clinic = partum[(partum["TermijnBijGeboorteInWeken"] < 30)
                | (partum["GeboorteGewichtInGram"] < 1000)
                | ((partum["GeboorteGewichtInGram"] < 1500) & (partum["OnderPercentiel"]))]
clinic_ids = clinic["Pseudo_id"].unique()
kneo_ids = appointment_requests.loc[appointment_requests["Orderopdracht"].str.contains("KNEO"), "Pseudo_id"].unique()
selected_ids = clinic_ids[~np.isin(clinic_ids, kneo_ids)]
# Patients that should get an appointment in the outpatient clinic
missing_appts = partum.loc[partum["Pseudo_id"].isin(selected_ids), ["Pseudo_id", "Voornaam", "Achternaam", "Geboortedatum"]]
# Full data to save
missing_data = koppeltabel.merge(missing_appts, on="Pseudo_id")
undefinable = koppeltabel.merge(undefinable[["Pseudo_id", "Voornaam", "Achternaam", "Geboortedatum"]], on="Pseudo_id")
# Fetch saved data
existing_missing_ids = pd.DataFrame(patient_selection_engine.execute('SELECT "Pseudo_id" FROM missing').fetchall())["Pseudo_id"]

# Save data
with pd.ExcelWriter("missing_patients.xlsx") as writer:
    # Upload to SQL
    missing_data[~missing_data["Pseudo_id"].isin(existing_missing_ids)].to_sql("missing", patient_selection_engine, if_exists="append", index=False)
    # Save to excel file
    missing_data.to_excel(writer, index=False)

# Save undefinable data
with pd.ExcelWriter("selection_undefinable.xlsx") as writer:
    # Save to excel file
    undefinable.to_excel(writer, index=False)