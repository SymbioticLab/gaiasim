import numpy as np
import sys

def percent_change(v1, v2):
    if v2 == 0:
        if v1 == 0:
            return 0
        return 1000000

    return abs(((v1 - v2) / v2) * 100.0)

def main():
    NUMPARAM = 2
    if len(sys.argv) != NUMPARAM + 1:
        print "ERROR: Incorrect number of command line arguments"
        print "Usage: python " + sys.argv[0] + " <sim1_results_file> <sim2_results_file>"
        print "  If sim1 and sim2 do not contain the same number of results, sim1 should be the smaller of the two"
        sys.exit(1)

    sim1_file = sys.argv[1]
    sim2_file = sys.argv[2]

    sim1_results = []
    with open(sim1_file, 'r') as infile:
        lines = infile.readlines()
   
    # Skip the first line because it just contains title info
    for line in lines[1:]:
        splits = line.split(',')
        id = splits[0].split('"')[1]
        jct = splits[3].split('"')[1]
        sim1_results.append((id, float(jct)))

    sim2_results = {}
    with open(sim2_file, 'r') as infile:
        lines = infile.readlines()
   
    # Skip the first line because it just contains title info
    for line in lines[1:]:
        splits = line.split(',')
        id = splits[0].split('"')[1]
        jct = splits[3].split('"')[1]
        sim2_results[id] = float(jct)

    x_labels = []
    y_vals = []
    for tup in sim1_results:
        id = tup[0]
        v2 = tup[1]

        if id in sim2_results:
            v1 = sim2_results[id]
            change = percent_change(v1, v2)
            print (id, change)
            x_labels.append(id)
            y_vals.append(change)

    sorted_diffs = sorted(y_vals)
    print "N\t%d" % len(sorted_diffs)
    print "mean\t%.2f" % np.mean(sorted_diffs)
    print "median\t%.2f" % np.median(sorted_diffs)
    print "stdev\t%.2f" % np.std(sorted_diffs)
    print "min\t%.2f" % min(sorted_diffs)
    print "max\t%.2f\tat %s" % (max(sorted_diffs), x_labels[y_vals.index(max(sorted_diffs))])

if __name__ == "__main__":
    main()
