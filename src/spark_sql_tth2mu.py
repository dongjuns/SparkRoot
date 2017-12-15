from __future__ import print_function
from __future__ import division
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import DataType
from array import array
import time

stime = time.time()

spark = SparkSession.builder \
        .appName("NanoAOD with Spark") \
        .config("spark.jars.packages","org.diana-hep:spark-root_2.11:0.1.11") \
        .getOrCreate()

sql = spark.sql

#data_directory = "/home/sunyoung/nano/NanoAOD/TTHAnalyzer/data/"
data_directory = "/home/nanoAOD/GluGlu_HToMuMu_M125_13TeV_powheg_pythia8/run2_2016MC_NANO_RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/171112_002433/0000/"

from pylab import *

rcParams['figure.figsize'] = (12.0,8.0)

file_name_p = "run2_2016MC_NANO_"
file_name_e = ".root"
file_name = ""
data_frame = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_1.root")

for i in range(2,4):
    file_name = file_name_p + str(i) + file_name_e
    temp = spark.read.format("org.dianahep.sparkroot").load(data_directory+file_name)
    data_frame = data_frame.union(temp)
    
    

data_frame.createOrReplaceTempView("NanoAOD")
data_frame.cache()

#data_frame.printSchema()

def selectMu(nMuon,Muon_Pt, Muon_Eta, Muon_pfRelIso04_all, Muon_tightId):
    result = dict()
    indices = []
    for i in range(nMuon):
        if(Muon_Pt[i] > 20 \
		and abs(Muon_Eta[i]) < 2.4 \
		and Muon_pfRelIso04_all < 0.25 \
		and Muon_tightId == True):
            indices.append(i)
    for j in range(len(indices)):
        
    result["Muon_Pt"] = Mu_Pt
    return result

spark.udf.register("selectMu", selectMu, MapType(StringType(),ArrayType(FloatType())))

MuonSelectionQuery = """select selectMu(nMuon,Muon_Pt,Muon_Eta,Muon_pfRelIso04_all, Muon_tightId) as Muon
			from NanoAOD
			where nMuon > 1
			"""
#			where nMuon > 1
#				and sort_array(Muon_Pt)[nMuon-1] > 20
#				and sort_array(Muon_Pt)[nMuon-2] > 20
#				and sort_array(Muon_Eta)[nMuon-1] between -2.4 and 2.4
#				and sort_array(Muon_Eta)[nMuon-2] between -2.4 and 2.4
#				and sort_array(Muon_pfRelIso04_all)[nMuon-1] < 0.25
#				and sort_array(Muon_pfRelIso04_all)[nMuon-2] < 0.25
#				and sort_array(Muon_tightId)[nMuon-1] = 1
#				and sort_array(Muon_tightId)[nMuon-2] = 1
#			"""

ElecSelectionQuery = """select Muon
			from DiMu
			"""

di_mu_selected_df = sql(MuonSelectionQuery)
di_mu_selected_df.createOrReplaceTempView("DiMu")
di_mu_selected_df.cache()

temp = sql(ElecSelectionQuery).count()

print("temp : ", str(temp))

#etime = time.time()

#out_f = open("/home/sunyoung/nano/NanoAOD/TTHAnalyzer/DiMuonCutTest.txt","w")
#result = "Muon-Cut test for a 'run2_2016MC_NANO_2.root' file : " + str(etime-stime) + " seconds"
#out_f.write(result)


