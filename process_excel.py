import pandas as pd
import numpy as np
import io

def process_excels(main_file, master_file):
    # ---------- Step 1: Read main file ----------
    data = pd.read_excel(main_file)

    # Rename columns to match desired
    column_mapping = {
        "HUB_CD": "HUB_CODE",
        "TOT_DAYS": "TOT_DAYS",
        "SAT_PROMISE": "SAT_PROMISE",
        "SUN_PROMISE": "SUN_PROMISE",
        "SAT_OVN_MOVE": "SAT_OVN_MOVE",
        "SUN_OVN_MOVE": "SUN_OVN_MOVE",
        "PLND_ORIG_HUB_CITY_NM": "HUB_CITY_NM"
    }
    data.rename(columns=column_mapping, inplace=True)

    desired_columns = [
        "SCAC_CD","LOC_NBR","DEST_ZIP_CD","ORIG_ZIP_CD",
        "LINE_HAUL_DAYS","HUB_TO_CUST_DAYS","TOT_DAYS",
        "HUB_CITY_NM","HUB_CODE","IS_ACTIVE",
        "SAT_DELV","SUN_DELV","SAT_PROMISE","SUN_PROMISE",
        "SAT_OVN_MOVE","SUN_OVN_MOVE","RGN","CAR_TYP",
        "EDGE_CALENDAR_ID"
    ]

    for col in desired_columns:
        if col not in data.columns:
            data[col] = np.nan

    # Convert promise columns
    promise_columns = ["SAT_PROMISE","SUN_PROMISE","SAT_OVN_MOVE","SUN_OVN_MOVE"]
    for col in promise_columns:
        if col in data.columns:
            data[col] = data[col].apply(lambda x: 1 if x is True else ("" if x is False else x))

    data = data[desired_columns]

    # HUB_CITY_NM → HUB_CODE mapping
    city_to_hub = {
        "COLUMBUS_FEDEX": 380,"COLUMBUS_UPS": 614,"CHICAGO_EARLY": 171,"CHICAGO_LATE": 191,
        "CCHIL_N": 290,"CCHIL_T": 292,"ROADIE_CHICAGO": 295,"EARLY_LOCAL": 180,"LATE_LOCAL": 220,
        "DALLAS_UPS_EARLY": 170,"DALLAS_UPS_LATE": 219,"ROADIE_DALLAS": 120,"BALTIMORE_FEDEX": 231,
        "BALTIMORE_UPS": 230,"ROADIE_BALTIMORE": 235,"FEDEX_HOUSTON": 211,"ROADIE_HOUSTON": 215,
        "UPS_HOUSTON": 210,"LACEY_ONTRAC": 400,"LACEY_FEDEX": 600,"LACEY_UPS": 500,
        "MIAMI_FEDEX": 305,"MIAMI_UPS": 954,"ROADIE_MIAMI": 970,"NEWARK_FEDEX_HOT": 661,
        "NEWARK_UPS_BAYN": 662,"ROADIE_NEWARK": 664,"TAMPA_FEDEX": 813,"TAMPA_FEDEX_OCAL": 815,
        "ROADIE_ORLANDO": 819,"ROADIE_TAMPABAY": 820,"TAMPA_UPS": 812,"TRACY_ONTRAC": 526,
        "TRACY_FEDEX": 524,"ROADIE_SANFRAN": 530,"TRACY_UPS": 528,"ATLANTA_FEDEX": 123,
        "BOSTON_FEDEX": 176,"BOSTON_UPS": 341,"BOSTON_UPS_T": 342,"FEDEX_LOCAL": 17,
        "FEDEX_LOCAL_COLUMBIA": 118,"EARTH_CITY_MO_T": 1,"ONTRAC_D": 3,"RIALTO_LATE": 111,
        "ONTARIO HUB": 112,"ONTRAC_N": 5,"ONTARIO HUB_CA_D": 2,"ROADIE_SANDIEGO": 115,
        "ROADIE_SOUTHLA": 110,"ELLENWOOD_EARLY": 19,"ELLENWOOD_LATE": 20,"ONTRAC_LOCAL": 301,
        "ONTRAC_LTSC": 302,"LOCAL": 9,"ROADIE_TROY_DET": 104,"HAGERSTOWN": 17,"GAITHERSBURG": 1,
        "ROADIE_HAGERSTOWN": 107,"ROADIE_LGMAIN": 100,"SMAGA": 1,"SMAGA_N": 1,"CACH": 1,"NBLOH": 2
    }
    data["HUB_CODE"] = data["HUB_CITY_NM"].map(city_to_hub)

    # LOC_NBR → ORIG_ZIP_CD mapping
    loc_to_zip = {
        5854: 8861, 5820: 60164, 6006: 92571, 6007: 92570, 5855: 33566, 5857: 95377,
        6707: 43443, 5829: 21219, 5882: 1876, 5523: 43162, 5823: 75211, 6760: 21740,
        5831: 77064, 5832: 98516, 6705: 30248, 6777: 30248, 5938: 65265, 5841: 33018, 5860: 30344
    }
    data["ORIG_ZIP_CD"] = data["LOC_NBR"].map(loc_to_zip).fillna(data["ORIG_ZIP_CD"])

    data["LINE_HAUL_DAYS"] = 0
    data["IS_ACTIVE"] = 1
    data["RGN"] = "NORTHLAKE01"
    data["CAR_TYP"] = "A"
    data["HUB_TO_CUST_DAYS"] = data["TOT_DAYS"]

    # ---------- Build Summary ----------
    summary_df = data[["SCAC_CD","LOC_NBR", "HUB_CITY_NM", "HUB_CODE"]].drop_duplicates()
    summary_df = summary_df.rename(columns={"HUB_CITY_NM": "HUB_City_Name","HUB_CODE": "HUB_CD"})
    summary_columns = [
        "Changes","SCAC_CD","LOC_NBR","HUB_City_Name","HUB_CD","Transit Days",
        "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday",
        "OECT","CPT","Scan Cut",
        "Same_Day_Delv_Monday","Same_Day_Delv_Tuesday","Same_Day_Delv_Wednesday",
        "Same_Day_Delv_Thursday","Same_Day_Delv_Friday","Same_Day_Delv_Saturday",
        "Same_Day_Delv_Sunday","DELTA_DAYS","IS_ACTIVE"
    ]
    for col in summary_columns:
        if col not in summary_df.columns:
            summary_df[col] = np.nan
    summary_df = summary_df[summary_columns]

    # ---------- Step 2: Read master.xlsx ----------
    master = pd.read_excel(master_file)
    master = master.rename(columns={
        "Hub Code":"HUB_CD",
        "HUB_CITY_NM":"HUB_City_Name",
        "Ship Schedule":"Ship_Schedule",
        "Order Entry Cut Time": "OECT",
        "Critical Pull Time": "CPT",
        "Master ScanCutTime": "Scan Cut"
    })
    master["HUB_City_Name_norm"] = master["HUB_City_Name"].str.strip().str.upper()
    master["HUB_CD_norm"] = master["HUB_CD"].astype(str).str.strip()
    summary_df["HUB_City_Name_norm"] = summary_df["HUB_City_Name"].str.strip().str.upper()
    summary_df["HUB_CD_norm"] = summary_df["HUB_CD"].astype(str).str.strip()

    # ---------- Step 3: Merge ----------
    merged = summary_df.merge(
        master[["HUB_City_Name_norm","HUB_CD_norm","Ship_Schedule","OECT","CPT","Scan Cut"]],
        on=["HUB_City_Name_norm","HUB_CD_norm"],
        how="left"
    )
    for col in ["OECT_x", "CPT_x", "Scan Cut_x"]:
        if col in merged.columns:
            merged.drop(columns=col, inplace=True)

    # Same-day delivery flags
    same_day_cols = [
        "Same_Day_Delv_Monday","Same_Day_Delv_Tuesday","Same_Day_Delv_Wednesday",
        "Same_Day_Delv_Thursday","Same_Day_Delv_Friday","Same_Day_Delv_Saturday",
        "Same_Day_Delv_Sunday"
    ]
    for col in same_day_cols:
        if col in merged.columns:
            merged[col] = merged["HUB_City_Name"].str.upper().str.contains("ROADIE")

    merged["DELTA_DAYS"] = 0
    merged["IS_ACTIVE"] = 1

    # ---------- Step 4: Populate weekdays ----------
    day_map = {
        "Monday":"M","Tuesday":"T","Wednesday":"W","Thursday":"Th",
        "Friday":"F","Saturday":"S","Sunday":"Su"
    }
    for day, token in day_map.items():
        merged[day] = merged["Ship_Schedule"].fillna("").apply(lambda x: token in x)
    merged["Transit Days"] = merged[list(day_map.keys())].sum(axis=1)
    merged.drop(columns=["HUB_City_Name_norm","HUB_CD_norm","Ship_Schedule"], inplace=True)

    summary_df = merged

    # ---------- Step 5: Export Excel (in memory) ----------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        sla_df = data.drop(columns=["EDGE_CALENDAR_ID"], errors="ignore")
        sla_df.to_excel(writer, index=False, sheet_name="SLA")

        for hub in data["HUB_CODE"].dropna().unique():
            hub_df = data[data["HUB_CODE"] == hub].copy()
            if "EDGE_CALENDAR_ID" not in hub_df.columns:
                hub_df["EDGE_CALENDAR_ID"] = np.nan
            hub_df.to_excel(writer, index=False, sheet_name=f"HUB{int(hub)}")

        summary_df.to_excel(writer, index=False, sheet_name="Summary")

    output.seek(0)
    return output
