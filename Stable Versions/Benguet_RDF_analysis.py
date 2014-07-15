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
    return R
   
        
    

reg_rdf=pd.read_csv('Benguet_RDF_regional.csv',header=0, index_col=0)
rain_dat=pd.read_csv('Benguet_rainfall_data.csv',header=0, index_col=0, dtype={'B0':np.float64})
plt.close('all')
for site in rain_dat.index[-5:-1]:
    
               
    MAP=fMAP(reg_rdf,rain_dat,site)
    print site,
    A=fA(reg_rdf,rain_dat,site)
    print A[0],A[1],A[2],MAP
    D=np.linspace(1/24.,3,24*3+2) #rainfall duration in days


    plt.figure()
    plt.title(site)
    plt.xlabel('Rainfall duration, D, (hours)')
    plt.ylabel('Rainfall mean intensity, I, (mm/hour)')
    for T in [0.5,1,2,5,10]:  #return period in years
        R=compute_plot_RDF(T,D,A)
        plt.plot(D*24.,R/(D*24.),'-', label=str(T)+"-year")

    
        
    plt.semilogy()
    plt.legend(fontsize='x-small')
    plt.grid(b=None, which='both', axis='both')
    plt.show()
    
