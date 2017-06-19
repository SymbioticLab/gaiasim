
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
#import statsmodels.api as sm # recommended import according to the docs

def process(csvfile):
    tmpfig = plt.figure()
    # read the csv and plot the last column
    data = pd.read_csv(csvfile)
    jct = data[data.columns[3]]
    jct = jct[jct != 0] # eliminate zeros!!
    #ecdf = sm.distributions.ECDF(jct)
    
    jct.hist(cumulative=True, normed=1, bins=1000000, histtype='step')
    
    avg = jct.mean()
    med = jct.quantile(0.5)
    p95 = jct.quantile(0.95)
    #tmpfig.hist(jct , cumulative=True, normed=1, bins=1000, histtype='step')
    
    # print the filename as the title
    plt.title(csvfile.name)
    plt.xlabel(data.columns[3])
    plt.ylabel('CDF')
    plt.axis([0.001 , jct.max(), 0, 1])

    plt.semilogx()  # use log for x-scale
    
    anno = 'Average: ' + str(avg) + ' s\n' \
        'Median: ' + str(med) + ' s\n' \
        '95%tile: ' + str(p95) + ' s\n' 
    
    plt.annotate(anno ,
            xy=(0.002, 0.8),        
            arrowprops=dict(facecolor='black', shrink=0.05),
            horizontalalignment='left',
            verticalalignment='bottom',
            clip_on=True)
    
    #plt.show()
    
    pp = PdfPages(csvfile.name + '.pdf')
    tmpfig.savefig(csvfile.name + '.png')
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
