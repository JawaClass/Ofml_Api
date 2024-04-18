#!/usr/bin/bash

cd /home/adm_pdf2obs/LIVE/Ofml_Api/

source venv/bin/activate

python -m scheduler_update_entry "/mnt/knps_testumgebung/ofml_development/repository"
