#!/bin/bash -eu
datapath=$1
report=${2:-"report.txt"}
MONTH=11

echo -n "Parsing smart recovery data..."
cat << EOF > $report
=======================================================
================= Smart Recovery Data =================
=======================================================

EOF
./parse_smart_data.py >> $report
echo "done"

echo -n "Parsing scrub data..."
cat << EOF >> $report
========================================================
===================== Scrubs Data ======================
========================================================

EOF
./parse_scrubs.py --month $MONTH --path $datapath >> $report
echo "done"

echo -n "Parsing slow request data..."
cat << EOF >> $report
========================================================
================== Slow Request Data ===================
========================================================

EOF
./parse_slow_requests.py --path $datapath >> $report
echo "done"

echo -n "Parsing suicide timeout data..."
cat << EOF >> $report
========================================================
================== Suicide Timeout Data ================
========================================================

EOF
./parse_suicides.py --month $MONTH --path $datapath >> $report
echo "done"

echo -e "\nReport written to $report."
