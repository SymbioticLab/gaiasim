#!/bin/bash
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# detect the hostname and set the rules
host=$(hostname -s)

case "$host" in
    "hk") $SCRIPTDIR/set_baserules_hk.sh ;;
    "la") $SCRIPTDIR/set_baserules_la.sh ;;
    "ny") $SCRIPTDIR/set_baserules_ny.sh ;;
    "fl") $SCRIPTDIR/set_baserules_fl.sh ;;
    "ba") $SCRIPTDIR/set_baserules_ba.sh ;;
    *) echo "Wrong hostname!"
    exit;;
esac

echo "deleting default route for 10.0.0.0/8"
sudo route del -net 10.0.0.0/8
