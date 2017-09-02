#!/bin/bash
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# first setup the ns
$SCRIPTDIR/set_netns_and_table.sh

# then detect the hostname and set the rules
host=$(hostname -s)

case "$host" in
    "hk") $SCRIPTDIR/set_iprules_hk.sh ;;
    "la") $SCRIPTDIR/set_iprules_la.sh ;;
    "ny") $SCRIPTDIR/set_iprules_ny.sh ;;
    "fl") $SCRIPTDIR/set_iprules_fl.sh ;;
    "ba") $SCRIPTDIR/set_iprules_ba.sh ;;
    *) echo "Wrong hostname!"
    exit;;
esac
