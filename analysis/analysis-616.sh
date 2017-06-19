#!/bin/sh

echo "Please run under directory gaiasim/analysis"

python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/base/half/bb/b2-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-half-bb.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/base/half/tpch/b2-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-half-tpch.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/base/half/fb/b2-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-half-fb.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/base/half/tpcds/b2-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-half-tpcds.log

echo "1/6"

python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/base/full/fb/b1-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-full-fb.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/base/full/tpcds/b1-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-full-tpcds.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/base/full/tpch/b1-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-full-tpch.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/base/full/bb/b1-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-full-bb.log

echo "2/6"

python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/base/quart/tpch/b4-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-quart-tpch.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/base/quart/fb/BW-b4-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-quart-fb.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/base/quart/bb/BW-b4-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-quart-bb.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/base/quart/tpcds/b4-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/base-quart-tpcds.log

echo "3/6"

python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/rrf/full/tpch/r1-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-full-tpch.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/rrf/full/bb/r1-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-full-bb.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/rrf/full/fb/r1-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-full-fb.log
python CalcUtil.py swan.filter /proj/gaia-PG0/gaia/results/6.16/rrf/full/tpcds/r1-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-full-tpcds.log

echo "4/6"

python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/rrf/quart/fb/r3-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-quart-fb.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/rrf/quart/bb/r3-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-quart-bb.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/rrf/quart/tpcds/r4-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-quart-tpcds.log
python CalcUtil.py swan-quart.filter /proj/gaia-PG0/gaia/results/6.16/rrf/quart/tpch/r4-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-quart-tpch.log

echo "5/6"

python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/rrf/half/fb/r2-fb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-half-fb.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/rrf/half/tpch/r2-tpch.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-half-tpch.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/rrf/half/tpcds/r2-tpcds.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-half-tpcds.log
python CalcUtil.py swan-half.filter /proj/gaia-PG0/gaia/results/6.16/rrf/half/bb/r2-bb.log > /proj/gaia-PG0/gaia/results/6.16/utilization/gaia-half-bb.log

echo "6/6"
