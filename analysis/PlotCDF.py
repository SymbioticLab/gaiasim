
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def process(csvfile):
    # read the csv and plot the last column
    data = pd.read_csv(csvfile)
    jct = data[data.columns[3]]
    jct = jct[jct != 0] # eliminate zeros!!
    jct.hist(cumulative=True, normed=1, bins=1000, histtype='step')
    
    # print the filename as the title
    plt.title(csvfile.name)
    plt.xlabel(data.columns[3])
    plt.ylabel('CDF')

    plt.semilogx()  # use log for x-scale
    #plt.show()
    
    pp = PdfPages(csvfile.name + '.pdf')
    plt.savefig(csvfile.name + '.png')
    plt.savefig(pp, format='pdf')
    pp.close()

def main():
    parser = argparse.ArgumentParser()      
    parser.add_argument('file', nargs='+', type=file)
    args = parser.parse_args()

    for f in args.file:
        print 'Now processing ' + f.name
        process(f)

if __name__ == '__main__':
    main()
