#!/bin/bash -eu
DATAPATH=""
REPORT=report.txt
MONTH=""

usage ()
{
    local opts=(
                 '--month <int>'
                 '--datapath <path>'
                 '[--output-file <path>]'
                )
    echo -n 'USAGE:'
    for opt in "${opts[@]}"; do
        echo -e "\t$opt"
    done
}

while (($# > 0))
do
    case "$1" in
    --month)
        MONTH="$2"
        shift
        ;;
    --output-file)
        REPORT="$2"
        shift
        ;;
    --datapath)
        DATAPATH="$2"
        shift
        ;;
    *)
        echo "ERROR: unknown option '$1'"
        usage
        exit 1
        ;;
    esac
    shift
done

if [ -z "$DATAPATH" ] || [ -z "$MONTH" ]; then usage; exit 1; fi


echo -n "Parsing smart recovery data..."
cat << EOF > $REPORT
=======================================================
================= Smart Recovery Data =================
=======================================================

EOF
./parse_smart_data.py >> $REPORT
echo "done"

echo -n "Parsing scrub data..."
cat << EOF >> $REPORT
========================================================
===================== Scrubs Data ======================
========================================================

EOF
./parse_scrubs.py --month $MONTH --path $DATAPATH >> $REPORT
echo "done"

echo -n "Parsing slow request data..."
cat << EOF >> $REPORT
========================================================
================== Slow Request Data ===================
========================================================

EOF
./parse_slow_requests.py --path $DATAPATH >> $REPORT
echo "done"

echo -n "Parsing suicide timeout data..."
cat << EOF >> $REPORT
========================================================
================== Suicide Timeout Data ================
========================================================

EOF
./parse_suicides.py --month $MONTH --path $DATAPATH >> $REPORT
echo "done"

echo -e "\nReport written to $REPORT."
