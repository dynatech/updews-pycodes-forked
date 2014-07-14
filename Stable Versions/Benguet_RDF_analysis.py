#DESCRIPTION: code for computing rainfall duration-frequency curves for sites in benguet

#based on: 
    #RAINFALL DURATION-FREQUENCY CURVE FOR UNGAGED
    #SITES IN THE HIGH RAINFALL, BENGUET MOUNTAIN
    #REGION IN THE PHILIPPINES

    #GUILLERMO Q. TABIOS III
        #Department of Civil Engineering, University of the Philippines
        #Diliman, Quezon City, Philippines 1101
    #DAVID S. ROJAS JR.
        #National Hydraulic Research Center, University of the Philippines
        #Diliman, Quezon City, Philippines 1101

    #56-FWR-M113, Proceedings of 2nd Asia PAcific Assoc of Hydrology and Water Resources (APHW) 2004
    

#modules and library
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


#functions
def fMAP(df,df2,site):
    #computes mean annual precipitation
    df=df[(df.index=='MAP')]

    df2=df2[(df2.index==site)]
    elev=df2['elev'].values[0]
    longi=df2['long'].values[0]
    lat=df2['lat'].values[0]

    MAP=df['B0'].values + df['B1'].values*elev + df['B2'].values*longi + df['B3'].values*lat
    return MAP[0]

def fA(df,df2,site):
    #computes parameters for specific RDF curve
    MAP=fMAP(df,df2,site)

    df2=df2[(df2.index==site)]
    elev=df2['elev'].values[0]
    longi=df2['long'].values[0]
    lat=df2['lat'].values[0]
    
    df3=df[(df.index!='MAP')]
    df3['Ai']=df3['B0'].values+ df3['B1'].values*MAP+ df3['B2'].values*elev+ df3['B3'].values*longi+ df3['B4'].values*lat

    return df3['Ai'].values

def compute_plot_RDF(T,D,A):
    R= A[0]* (T**A[1]) * (D**A[2])
    plt.plot(R,T,'.-', label=str(D)+"-day")
        
    

reg_rdf=pd.read_csv('Benguet_RDF_regional.csv',header=0, index_col=0)
rain_dat=pd.read_csv('Benguet_rainfall_data.csv',header=0, index_col=0, dtype={'B0':np.float64})
plt.close('all')
for site in rain_dat.index[-5:-1]:
    plt.figure()
    plt.title(site)
    plt.ylabel('Return period, T, (years)')
    plt.xlabel('Total rainfall, R, (mm)')
               
    MAP=fMAP(reg_rdf,rain_dat,site)
    print site,MAP,
    A=fA(reg_rdf,rain_dat,site)
    print A
    T=np.asarray([0.5,1,2,5,10,20,50,100,200,500]) #return period in years
    for D in [1,2,3]:  #rainfall duration in days
        compute_plot_RDF(T,D,A)
    plt.loglog()
    plt.legend(fontsize='x-small')
    plt.grid(b=None, which='both', axis='both')
plt.show()
    
