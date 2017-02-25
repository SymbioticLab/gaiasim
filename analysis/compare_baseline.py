import numpy
import sys

def main():
    NUMPARAM = 2
    if len(sys.argv) != NUMPARAM + 1:
        print "ERROR: Incorrect number of command line arguments"
        print "Usage: python " + sys.argv[0] + " <baseline_results_file> <coflow_results_file>"
        sys.exit(1)
    
    sim1_file = sys.argv[1]
    sim2_file = sys.argv[2]

    sim1_results = []
    sim1_results_no_id = []
    with open(sim1_file, 'r') as infile:
        lines = infile.readlines()
   
    # Skip the first line because it just contains title info
    for line in lines[1:]:
        splits = line.split(',')
        id = splits[0].split('"')[1]
        jct = splits[3].split('"')[1]
        sim1_results.append((id, float(jct)))
        sim1_results_no_id.append(float(jct))

    sim2_results = {}
    sim2_results_no_id = []
    with open(sim2_file, 'r') as infile:
        lines = infile.readlines()
   
    # Skip the first line because it just contains title info
    for line in lines[1:]:
        splits = line.split(',')
        id = splits[0].split('"')[1]
        jct = splits[3].split('"')[1]
        sim2_results[id] = float(jct)
        sim2_results_no_id.append(float(jct))

    mean_sim1 = numpy.mean(sim1_results_no_id)
    mean_sim2 = numpy.mean(sim2_results_no_id)
    print "baseline_mean / coflow_mean = %.2f" % (mean_sim1 / mean_sim2)

    sorted_sim1 = sorted(sim1_results_no_id)
    sorted_sim2 = sorted(sim2_results_no_id)
    sim1_95 = numpy.percentile(sorted_sim1, 95)
    sim2_95 = numpy.percentile(sorted_sim2, 95)
    print "baseline_95 / coflow_95 = %.2f" % (sim1_95 / sim2_95)

if __name__ == "__main__":
    main()
