#!/bin/bash

echo "Running: GTT Processor"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "DEL_GTT_INS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "INS_GTT_INS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "GTT_INS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "ALTER_GTT_INS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "INS_GTT_INS"

echo "✅ All tasks completed."
