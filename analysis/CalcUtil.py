
# Format of filter conf file: [interface] [if_out] [if_in] [bw] (eth1 1 1 1280)


import argparse
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib.backends.backend_pdf import PdfPages
#import statsmodels.api as sm # recommended import according to the docs

# reads the csvfile, and apply filters to calculate utilization
def process(filters, csvfile):
    data = pd.read_csv(csvfile, header = None)
    startTime = data[0].min()
    endTime = data[0].max()
    
    for i in range(startTime, endTime + 1):
        
        print str(i) + ' ' + str(utilByTimestamp(data, i, filters))
        

# sum up all interfaces for this timestamp
def utilByTimestamp(data, timestamp, filters):
    tmp_data = data[ data[0] == timestamp]
    
    # 2 bytes_out/s 3 bytes_in/s
    out_rate = tmp_data.groupby(1).agg(np.mean)[2]
    in_rate = tmp_data.groupby(1).agg(np.mean)[3]
    
    totalBW = 0.0
    usedBW = 0.0
    
    # for each interface in filter, sum up the totalBW and calculate the utilization
    for interface in filters:
        # don't check the nullness. Fail if happens
        try:
            if filters[interface][0] == 1:
                usedBW += out_rate[interface]
                totalBW += filters[interface][2]
            if filters[interface][1] == 1:
                usedBW += in_rate[interface]
                totalBW += filters[interface][2]
        except:
            print('Oops, interface ' + interface + ' not found at time: ' + str(timestamp) )
            break
    
    
    if totalBW != 0:
        utilization = usedBW / totalBW
    else:
        utilization = 0.0
            
    return utilization
    
def readFilterConf(filterConf):
    ret = {}
    
    for line in filterConf:
        split = line.split(' ')
        if len(split) == 4:
            #print split[0]
            #print split[1]
            #print split[2]
            #print split[3]
            ret[split[0]] = [ int(split[1]) , int(split[2]) , float(split[3])]
            
    return ret


def main():
    parser = argparse.ArgumentParser()      
    parser.add_argument('filterFile', help='Path of filter config file', type=file)
    parser.add_argument('csv', help='Path of csv file to parse', type=file)    
    args = parser.parse_args()
    
    filterConf = readFilterConf(args.filterFile)
    process(filterConf, args.csv)

if __name__ == '__main__':
    main()
