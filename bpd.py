import datetime
import numpy as np
import pandas as pd
import subprocess

PAST_MONTHS = 3


def compute_factor(weight, flow):
    if flow <= 0.01:
        if 2000 < weight:
            return 0
        else:
            return 1
    elif 0.01 < flow <= 0.03:
        if weight <= 1000:
            return 3
        elif 1000 < weight <= 2000:
            return 2
        else:
            return 1
    elif 0.03 < flow <= 0.06:
        if weight <= 1000:
            return 6
        elif 1000 < weight <= 1250:
            return 5
        elif 1250 < weight <= 1500:
            return 4
        elif 1500 < weight <= 2000:
            return 3
        else:
            return 2
    elif 0.06 < flow <= 0.125:
        if weight <= 1000:
            return 12
        elif 1000 < weight <= 1250:
            return 10
        elif 1250 < weight <= 1500:
            return 8
        elif 1500 < weight <= 2000:
            return 6
        else:
            return 4
    elif 0.125 < flow <= 0.15:
        if weight <= 1000:
            return 15
        elif 1000 < weight <= 1250:
            return 12
        elif 1250 < weight <= 1500:
            return 10
        elif 1500 < weight <= 2000:
            return 8
        elif 2000 < weight <= 2500:
            return 6
        else:
            return 5
    elif 0.15 < flow <= 0.25:
        if weight <= 1000:
            return 25
        elif 1000 < weight <= 1250:
            return 20
        elif 1250 < weight <= 1500:
            return 17
        elif 1500 < weight <= 2000:
            return 13
        elif 2000 < weight <= 2500:
            return 10
        else:
            return 8
    elif 0.25 < flow <= 0.5:
        if weight <= 1000:
            return 50
        elif 1000 < weight <= 1250:
            return 40
        elif 1250 < weight <= 1500:
            return 33
        elif 1500 < weight <= 2000:
            return 25
        elif 2000 < weight <= 2500:
            return 20
        else:
            return 17
    elif 0.5 < flow <= 0.75:
        if weight <= 1000:
            return 75
        elif 1000 < weight <= 1250:
            return 60
        elif 1250 < weight <= 1500:
            return 50
        elif 1500 < weight <= 2000:
            return 38
        elif 2000 < weight <= 2500:
            return 30
        else:
            return 25
    elif 0.75 < flow < 1:
        if weight <= 1000:
            return 100
        elif 1000 < weight <= 1250:
            return 80
        elif 1250 < weight <= 1500:
            return 67
        elif 1500 < weight <= 2000:
            return 50
        elif 2000 < weight <= 2500:
            return 40
        else:
            return 33
    else:
        return 999


def compute_fio2_effect(factor, fio2):
    if fio2 == 21 or fio2 == 22:
        if factor < 80:
            return 21
    elif 23 <= fio2 < 26:
        if 0 <= factor < 13:
            return 21
        elif 13 <= factor < 37:
            return 22
        elif 37 <= factor < 80:
            return 23
    elif 26 <= fio2 < 31:
        if 0 <= factor < 6:
            return 21
        elif 6 <= factor < 17:
            return 22
        elif 17 <= factor < 28:
            return 23
        elif 28 <= factor < 39:
            return 24
        elif 39 <= factor < 80:
            return 25
    elif 31 <= fio2 < 41:
        if 0 <= factor < 3:
            return 21
        elif 3 <= factor < 8:
            return 22
        elif 8 <= factor < 14:
            return 23
        elif 14 <= factor < 19:
            return 24
        elif 19 <= factor < 28:
            return 25
        elif factor == 28:
            return 26
        elif 29 <= factor < 36:
            return 27
        elif 36 <= factor < 40:
            return 28
        elif 40 <= factor < 80:
            return 29
    elif 41 <= fio2 < 51:
        if 0 <= factor < 2:
            return 21
        elif 2 <= factor < 6:
            return 22
        elif 6 <= factor < 9:
            return 23
        elif 9 <= factor < 13:
            return 24
        elif 13 <= factor < 17:
            return 25
        elif 17 <= factor < 19:
            return 26
        elif 10 <= factor < 23:
            return 27
        elif 23 <= factor < 27:
            return 28
        elif 27 <= factor < 30:
            return 29
        elif factor == 30:
            return 30
        elif 31 <= factor < 37:
            return 31
        elif 37 <= factor < 39:
            return 32
        elif 39 <= factor < 44:
            return 33
    elif 51 <= fio2 < 100:
        if 0 <= factor < 1:
            return 21
        elif 1 <= factor < 2:
            return 22
        elif 2 <= factor < 4:
            return 23
        elif factor == 4:
            return 24
        elif factor == 5:
            return 25
        elif factor == 6:
            return 26
        elif 7 <= factor < 9:
            return 27
        elif factor == 9:
            return 28
        elif factor == 10:
            return 29
        elif 11 <= factor < 13:
            return 30
        elif factor == 13:
            return 31
        elif factor == 14:
            return 32
        elif factor == 15:
            return 33
        elif 16 <= factor < 18:
            return 34
        elif factor == 18:
            return 35
        elif factor == 19:
            return 36
        elif factor == 20:
            return 37
        elif 21 <= factor < 23:
            return 38
        elif factor == 23:
            return 39
        elif factor == 25:
            return 41
        elif factor == 27:
            return 42
        elif factor == 28:
            return 43
        elif factor == 29:
            return 44
        elif factor == 30:
            return 45
        elif 31 <= factor < 36:
            return 47
        elif factor == 36:
            return 49
        elif factor == 38:
            return 51
        elif factor == 40:
            return 53

    return 999


def bpd_diagnosis(ondst36wpma, flow36wpma, fio236wpma, fio2_effect):
    if ondst36wpma == 0:
        return 1
    elif ondst36wpma in [1, 2, 5, 6]:
        return 4
    elif ondst36wpma == 3 and flow36wpma <= 1 and fio236wpma == 21:
        return 2
    elif ondst36wpma == 3 and flow36wpma <= 1 and 21 < fio236wpma <= 30:
        return 3
    elif ondst36wpma == 3 and flow36wpma > 1:
        return 4
    elif ondst36wpma == 7 and fio2_effect == 21:
        return 2
    elif ondst36wpma == 7 and 21 < fio2_effect <= 30:
        return 3
    elif ondst36wpma == 7 and 30 < fio2_effect <= 100:
        return 4
    else:
        return 999


# Ignore warnings
np.seterr(invalid="ignore")

# Get data from rdp
subprocess.call(r"\\amc.intra\users\O\opalafoxverna\home\R-Portable\App\R-Portable\bin\Rscript.exe --vanilla ./rdp.r",
                shell=True)

# Load patient number
koppeltabel = pd.read_csv(
    "data/koppeltabel.csv").drop(columns=["Unnamed: 0", "CreatePseudoIdDT"])

# Load oxygen data
oxygen = pd.read_csv(
    "data/oxygen.csv")[["Pseudo_id", "TotaalAantalBehandeldagenO2Kind"]]
# Keep highest "TotaalAantalBehandeldagenO2Kind" value when duplicate present
oxygen = oxygen.sort_values(
    "TotaalAantalBehandeldagenO2Kind", ascending=False).drop_duplicates("Pseudo_id")

# Load appointment ordertype data
appointment_ordertype = pd.read_csv(
    "data/appointments_ordertype.csv").drop(columns=["Unnamed: 0", "uitgifteDT"])
# Pivot data and extract useful questions
appointment_ordertype = (appointment_ordertype.pivot(index=["OrderId", "Pseudo_id", "OrderOmschrijving"],
                                                     columns="OrderSpecificatievraagFormulering",
                                                     values="OrderSpecificatievraagAntwoord")[["Welk spreekuur?", "Vragenlijst?"]]
                         .dropna(how="all")
                         .reset_index())

# Load bpd data
bpd = (pd.read_csv("data/bpd.csv", parse_dates=["MeetMoment", "MeetDatum", "MeetTijd", "NICU_36_WKN_ASSESSMENT_DATUM_3940007331"])
       .drop(columns=["Unnamed: 0", "uitgifteDT"]))
# Compute start assessment date
start_assessment_date = datetime.datetime.utcnow(
) - datetime.timedelta(PAST_MONTHS * 31)
# Remove children assessed before desired time range
bpd = bpd.drop(bpd[bpd["NICU_36_WKN_ASSESSMENT_DATUM_3940007331"]
                   < start_assessment_date].index)
# Remove duplicates, keeping the row with the least NaNs
bpd = (bpd.assign(counts=bpd.count(axis=1))
          .sort_values(["Pseudo_id", "counts"])
          .drop_duplicates("Pseudo_id", keep="last")
          .drop(columns="counts"))
# Clean data
bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"] = (bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"].str.replace(",", ".")
                                                                                                    .str.extract(r"(\d{4}|\d{1}.\d*)"))
kg_mask = bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"].str.contains(
    ".", regex=False).fillna(False)
bpd.loc[kg_mask, "NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"] = (bpd.loc[kg_mask, "NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"].astype(float)
                                                                 * 1000).astype(int)
bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"] = pd.to_numeric(
    bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"])


bpd["flow36wpma"] = (bpd["NICU_36_WKN_ASSESSMENT_PEEP_FLOW_3940007335"].str.replace(" ", "", regex=False)
                                                                       .str.replace(",", ".", regex=False)
                                                                       .str.replace(r"3\d{1}\+\d{1}", "", regex=True))
bpd["flow36wpma"] = (bpd["flow36wpma"].str.extract(r"(\d{1}(\.\d*)?((?i)l)?)")[0]
                                      .str.replace("(?i)l", "", regex=True)
                                      .astype(float))

bpd["fio236wpma"] = (bpd["NICU_36_WKN_ASSESSMENT_FIO2_3940007336"].str.replace(",", ".", regex=False)
                                                                  .str.replace(r"\d{4}", "", regex=True)
                                                                  .str.replace("O2", "", regex=False))
bpd["fio236wpma"] = bpd["fio236wpma"].str.extract(
    r"((\d{1}\.\d*)|(\d{1,3}))")[0].astype(float)
bpd.loc[bpd["fio236wpma"] < 1,
        "fio236wpma"] = bpd.loc[bpd["fio236wpma"] < 1, "fio236wpma"] * 100


compute_factor_vec = np.vectorize(compute_factor)

bpd["factor"] = compute_factor_vec(bpd["NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339"],
                                   bpd["flow36wpma"])

compute_fio2_effect_vec = np.vectorize(compute_fio2_effect)
bpd["fio2_effect"] = compute_fio2_effect_vec(bpd["factor"], bpd["fio236wpma"])

transformations = {
    "Geen ondersteuning": 0,
    "CPAP": 1,
    "HHHFNC": 2,
    "Snor": 3,
    "Invasieve beademing": 6,
    "nIMV": 6,
    "Low flow": 7
}
bpd["ondst36wpma"] = bpd["NICU_36_WKN_ASSESSMENT_MODUS_3940007334"].replace(transformations.keys(),
                                                                            transformations.values(),
                                                                            regex=True)
# Track patients that cannot be automatically selected
undefinable = bpd[~bpd["Pseudo_id"].isin(oxygen["Pseudo_id"])]
bpd = bpd.merge(oxygen, on="Pseudo_id")
# Compute the maximum between the two ignoring NaNs
bpd["oxygen"] = np.fmax(bpd["NICU_36_WKN_ASSESSMENT_TOTAAL_AANTAL_O2_DAGEN_3046501667"],
                        bpd["TotaalAantalBehandeldagenO2Kind"],
                        dtype=int,
                        casting="unsafe")


bpd_diagnosis_vec = np.vectorize(bpd_diagnosis)
bpd["diagnosis"] = bpd_diagnosis_vec(bpd["ondst36wpma"],
                                     bpd["flow36wpma"],
                                     bpd["fio236wpma"],
                                     bpd["fio2_effect"])

# Override some of Wes logic with days of oxygen
bpd.loc[bpd["oxygen"] < 28, "diagnosis"] = 1
bpd.loc[(bpd["oxygen"] > 28) & ((bpd["fio236wpma"].isnull())
                                | (bpd["fio236wpma"] <= 21)), "diagnosis"] = 2
bpd.loc[(bpd["oxygen"] > 28) & (bpd["fio236wpma"].between(
    21, 30, inclusive="neither")), "diagnosis"] = 3
bpd.loc[(bpd["oxygen"] > 28) & ((bpd["fio236wpma"] >= 30)
                                | (bpd["flow36wpma"] > 1)
                                | (bpd["ondst36wpma"] == 2)), "diagnosis"] = 4

# Save patients that could not be diagnosed automatically
undefinable = pd.concat([undefinable, bpd[bpd["diagnosis"] == 999]])
bpd = bpd.drop(bpd[bpd["diagnosis"] == 999].index)

bpd_diagnosis_map = {
    1: "geen BPD",
    2: "milde BPD",
    3: "matig ernstige BPD",
    4: "ernstige BPD"
}
bpd["diagnosis"] = bpd["diagnosis"].replace(bpd_diagnosis_map)

# Add patient number
bpd = koppeltabel.merge(bpd, on="Pseudo_id")
undefinable = koppeltabel.merge(undefinable, on="Pseudo_id")

# Select a subset of all columns
bpd = bpd[["Pseudo_id", "Patientnummer", "diagnosis",
           "NICU_36_WKN_ASSESSMENT_CONCLUSIE_3940007338"]]
undefinable = undefinable[[
    "Pseudo_id",
    "Patientnummer",
    "MeetMoment",
    "NICU_36_WKN_ASSESSMENT_GEWICHT_3940007339",
    "NICU_36_WKN_ASSESSMENT_PEEP_FLOW_3940007335",
    "NICU_36_WKN_ASSESSMENT_FIO2_3940007336",
    "diagnosis"
]]

# Save patients for which the BPD diagnosis was wrong
wrong_appts = bpd[bpd["diagnosis"] != bpd["NICU_36_WKN_ASSESSMENT_CONCLUSIE_3940007338"]]
with pd.ExcelWriter("bpd_diagnosis_diff.xlsx") as writer:
    # Save to excel file
    wrong_appts.to_excel(writer, index=False)

# Save undefinable data
with pd.ExcelWriter("bpd_undefinable.xlsx") as writer:
    # Save to excel file
    undefinable.to_excel(writer, index=False)

# Extract patients with moderate or severe BPD
severe_bpd = bpd[bpd["diagnosis"].isin(["matig ernstige BPD", "ernstige BPD"])]
# Get patients that did not receive any appointment at the outpatient clinic
bpd_appt_missing = severe_bpd[
    ~severe_bpd["Pseudo_id"].isin(appointment_ordertype.loc[appointment_ordertype["OrderOmschrijving"].str.contains("KNEO FOLLOW ME"),
                                                            "Pseudo_id"])
]
# Merge bpd data with appointment data
bpd_appt = severe_bpd.merge(appointment_ordertype, on="Pseudo_id")
# Get order ids with incorrect appointment order type for patient's BPD diagnosis or incorrect BPD formatting
wrong_bpd_appt = bpd_appt.loc[(~bpd_appt["Welk spreekuur?"].isin(["A", "C", "E", "H", "J"]))
                              | (bpd_appt["Vragenlijst?"] != "BPD")]
wrong_bpd_appt = pd.concat([bpd_appt_missing, wrong_bpd_appt])
wrong_bpd_appt = wrong_bpd_appt.astype({
    "OrderId": "Int64"
})

# Save wrong BPD appointments data
with pd.ExcelWriter("bpd_wrong_appts.xlsx") as writer:
    # Save to excel file
    wrong_bpd_appt.to_excel(writer, index=False)
