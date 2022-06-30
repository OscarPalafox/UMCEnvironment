import datetime
import subprocess
import numpy as np
import pandas as pd


PAST_MONTHS = 3


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

# Get data from rdp
subprocess.call(r"\\amc.intra\users\O\opalafoxverna\home\R-Portable\App\R-Portable\bin\Rscript.exe --vanilla ./rdp.r",
                shell=True)

# Load patient data
patient = (pd.read_csv("data/patient.csv", parse_dates=["Geboortedatum", "Overlijdensdatum"])
             .drop(columns=["Unnamed: 0", "uitgifteDT"]))
patient["IsOverleden"] = ~patient["Overlijdensdatum"].isnull()

# Load prn data
prn = get_prn_data("data/PRN-data.xlsx")

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
undefinable = undefinable.merge(patient, on="Pseudo_id")
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

# Load patient requests
appointment_requests = (pd.read_csv("data/appointments_requests.csv", parse_dates=["AanvraagDatum", "IngeplandDatum"])
                          .drop(columns=["Unnamed: 0", "uitgifteDT"]))

# Load patient number
koppeltabel = pd.read_csv("data/koppeltabel.csv").drop(columns=["Unnamed: 0", "CreatePseudoIdDT"])


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

# Save data
with pd.ExcelWriter("missing_patients.xlsx") as writer:
    # Save to excel file
    missing_data.to_excel(writer, index=False)
    
# Save undefinable data
with pd.ExcelWriter("selection_undefinable.xlsx") as writer:
    # Save to excel file
    undefinable.to_excel(writer, index=False)