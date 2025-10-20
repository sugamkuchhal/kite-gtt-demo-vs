#!/usr/bin/env python3

SHEET_ID = "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI"
RANGE = "ALL_OLD_GTTs!R1"

def is_trigger_true():
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file("creds_vs.json", scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEET_ID)
        result = sheet.values_get(RANGE, params={"valueRenderOption": "FORMATTED_VALUE"})
        value = result.get("values", [[""]])[0][0]

        return str(value).strip().lower() == "true"
    except Exception:
        return False

if __name__ == "__main__":
    print(is_trigger_true())
