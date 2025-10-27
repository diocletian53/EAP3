import pandas as pd
import numpy as np

def process_excel(main_file_path, master_file_path, output_file_path):
    """
    Process main and master Excel files:
    - Preserve existing HUB_CD
    - Map HUB_CITY_NM → HUB_CD for missing values
    - Map LOC_NBR → ORIG_ZIP_CD
    - Merge master info (OECT/CPT/Scan Cut)
    - Export SLA, HUB### sheets, and Summary
    """

    # =========================
    # STEP 1: READ MAIN FILE
    # =========================
    data = pd.read_excel(main_file_path)

    # Standardize column names
    column_mapping = {
        "PLND_ORIG_HUB_CITY_NM": "HUB_CITY_NM",
        "HUB_CD": "HUB_CD"
    }
    data.rename(columns=column_mapping, inplace=True)

    # Ensure boolean fields are normalized
    for col in ["SAT_PROMISE","SUN_PROMISE","SAT_OVN_MOVE","SUN_OVN_MOVE"]:
        if col in data.columns:
            data[col] = data[col].apply(lambda x: 1 if x is True else ("" if x is False else x))

    # Fill missing required columns
    desired_columns = [
        "SCAC_CD","LOC_NBR","DEST_ZIP_CD","ORIG_ZIP_CD",
        "LINE_HAUL_DAYS","HUB_TO_CUST_DAYS","TOT_DAYS",
        "HUB_CITY_NM","HUB_CD","IS_ACTIVE",
        "SAT_PROMISE","SUN_PROMISE","SAT_OVN_MOVE","SUN_OVN_MOVE",
        "RGN","CAR_TYP","EDGE_CALENDAR_ID"
    ]
    for col in desired_columns:
        if col not in data.columns:
            data[col] = np.nan

    # =========================
    # STEP 2: MAP HUB CODES
    # =========================
    city_to_hub = {
        "COLUMBUS_FEDEX": 380, "COLUMBUS_UPS": 614, "CHICAGO_EARLY": 171,
        "CHICAGO_LATE": 191, "CCHIL_N": 290, "CCHIL_T": 292, "ROADIE_CHICAGO": 295,
        "EARLY_LOCAL": 180, "LATE_LOCAL": 220, "DALLAS_UPS_EARLY": 170, "DALLAS_UPS_LATE": 219,
        "ROADIE_DALLAS": 120, "BALTIMORE_FEDEX": 231, "BALTIMORE_UPS": 230, "ROADIE_BALTIMORE": 235,
        "FEDEX_HOUSTON": 211, "ROADIE_HOUSTON": 215, "UPS_HOUSTON": 210, "LACEY_ONTRAC": 400,
        "LACEY_FEDEX": 600, "LACEY_UPS": 500, "MIAMI_FEDEX": 305, "MIAMI_UPS": 954, "ROADIE_MIAMI": 970,
        "NEWARK_FEDEX_HOT": 661, "NEWARK_UPS_BAYN": 662, "ROADIE_NEWARK": 664, "TAMPA_FEDEX": 813,
        "TAMPA_FEDEX_OCAL": 815, "ROADIE_ORLANDO": 819, "ROADIE_TAMPABAY": 820, "TAMPA_UPS": 812,
        "TRACY_ONTRAC": 526, "TRACY_FEDEX": 524, "ROADIE_SANFRAN": 530, "TRACY_UPS": 528,
        "ATLANTA_FEDEX": 123, "BOSTON_FEDEX": 176, "BOSTON_UPS": 341, "BOSTON_UPS_T": 342,
        "FEDEX_LOCAL": 17, "FEDEX_LOCAL_COLUMBIA": 118, "EARTH_CITY_MO_T": 1, "ONTRAC_D": 3,
        "RIALTO_LATE": 111, "ONTARIO HUB": 112, "ONTRAC_N": 5, "ONTARIO HUB_CA_D": 2,
        "ROADIE_SANDIEGO": 115, "ROADIE_SOUTHLA": 110, "ELLENWOOD_EARLY": 19, "ELLENWOOD_LATE": 20,
        "ONTRAC_LOCAL": 301, "ONTRAC_LTSC": 302, "LOCAL": 9, "ROADIE_TROY_DET": 104,
        "HAGERSTOWN": 17, "GAITHERSBURG": 1, "ROADIE_HAGERSTOWN": 107,
        "ROADIE_LGMAIN": 100, "SMAGA": 1, "SMAGA_N": 1, "CACH": 1, "NBLOH": 2
    }

    mapped_hubs = data["HUB_CITY_NM"].str.strip().str.upper().map(city_to_hub)
    data["HUB_CD"] = np.where(data["HUB_CD"].isna(), mapped_hubs, data["HUB_CD"])
    data["HUB_CD"] = pd.to_numeric(data["HUB_CD"], errors="coerce")
    data["HUB_MAPPING_STATUS"] = np.where(data["HUB_CD"].isna(), "UNMAPPED", "OK")

    # =========================
    # STEP 3: MAP ZIP CODES
    # =========================
    loc_to_zip = {
        5854: 8861, 5820: 60164, 6006: 92571, 6007: 92570,
        5855: 33566, 5857: 95377, 6707: 43443, 5829: 21219,
        5882: 1876, 5523: 43162, 5823: 75211, 6760: 21740,
        5831: 77064, 5832: 98516, 6705: 30248, 6777: 30248,
        5938: 65265, 5841: 33018, 5860: 30344
    }
    data["ORIG_ZIP_CD"] = data["LOC_NBR"].map(loc_to_zip).fillna(data["ORIG_ZIP_CD"])

    # =========================
    # STEP 4: DEFAULT FIELDS
    # =========================
    data["LINE_HAUL_DAYS"] = 0
    data["IS_ACTIVE"] = 1
    data["RGN"] = "NORTHLAKE01"
    data["CAR_TYP"] = "A"
    data["HUB_TO_CUST_DAYS"] = data["TOT_DAYS"]

    # =========================
    # STEP 5: MERGE MASTER DATA
    # =========================
    summary_df = data[["SCAC_CD","LOC_NBR","HUB_CITY_NM","HUB_CD"]].drop_duplicates()
    summary_df.rename(columns={"HUB_CITY_NM":"HUB_City_Name"}, inplace=True)

    master = pd.read_excel(master_file_path)
    master.rename(columns={
        "Hub Code":"HUB_CD","HUB_CITY_NM":"HUB_City_Name",
        "Ship Schedule":"Ship_Schedule","Order Entry Cut Time":"OECT",
        "Critical Pull Time":"CPT","Master ScanCutTime":"Scan Cut"
    }, inplace=True)

    # Normalize keys
    master["HUB_CD_norm"] = master["HUB_CD"].astype(str).str.strip()
    summary_df["HUB_CD_norm"] = summary_df["HUB_CD"].astype(str).str.strip()

    master = master.drop_duplicates(subset=["HUB_CD_norm"])
    summary_df = summary_df.merge(master[["HUB_CD_norm","Ship_Schedule","OECT","CPT","Scan Cut"]],
                                  on="HUB_CD_norm", how="left")

    # =========================
    # STEP 6: EXPORT TO EXCEL
    # =========================
    unique_hubs = data["HUB_CD"].dropna().unique()
    with pd.ExcelWriter(output_file_path, engine="openpyxl") as writer:
        data.drop(columns=["EDGE_CALENDAR_ID"], errors="ignore").to_excel(writer,index=False,sheet_name="SLA")
        for hub in unique_hubs:
            hub_df = data[data["HUB_CD"]==hub].copy()
            if "EDGE_CALENDAR_ID" not in hub_df.columns:
                hub_df["EDGE_CALENDAR_ID"] = np.nan
            hub_df.to_excel(writer,index=False,sheet_name=f"HUB{int(hub)}")
        summary_df.to_excel(writer,index=False,sheet_name="Summary")

    print("✅ Process complete. Check HUB_MAPPING_STATUS for unmapped hubs if any.")
