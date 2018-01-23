#!/bin/bash
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# first setup the ns
$SCRIPTDIR/set_netns_and_table_baseline.sh
# detect the hostname and set the rules
host=$(hostname -s)

case "$host" in
    "hk") $SCRIPTDIR/set_mptcprules_hk.sh ;;
    "la") $SCRIPTDIR/set_mptcprules_la.sh ;;
    "ny") $SCRIPTDIR/set_mptcprules_ny.sh ;;
    "fl") $SCRIPTDIR/set_mptcprules_fl.sh ;;
    "ba") $SCRIPTDIR/set_mptcprules_ba.sh ;;
    *) echo "Wrong hostname!"
    exit;;
esac

#echo "deleting default route for 10.0.0.0/8"
#sudo route del -net 10.0.0.0/8
