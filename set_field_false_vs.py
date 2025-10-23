from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI"
SHEET_NAME = "ALL_OLD_GTTs"
CELL = "R1"

def main():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    body = {
        "values": [["FALSE"]]
    }

    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!{CELL}",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

    print(f"Updated {CELL} in {SHEET_NAME} to FALSE")

if __name__ == "__main__":
    main()
