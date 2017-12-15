from __future__ import print_function
from __future__ import division
from pyspark.sql import SparkSession

import time

stime = time.time()

spark = SparkSession.builder \
        .appName("NanoAOD with Spark") \
        .config("spark.jars.packages","org.diana-hep:spark-root_2.11:0.1.11") \
        .getOrCreate()

sql = spark.sql

data_directory = "/home/sunyoung/nano/NanoAOD/TTHAnalyzer/data/"

from pylab import *

rcParams['figure.figsize'] = (12.0,8.0)

data_frame = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_2.root")

data_frame.createOrReplaceTempView("NanoAOD")
data_frame.cache()

#data_frame.printSchema()


MuonSelectionQuery = """select *
			from NanoAOD
			where nMuon > 1
				and sort_array(Muon_Pt)[nMuon-1] > 20
				and sort_array(Muon_Pt)[nMuon-2] > 20
				and sort_array(Muon_Eta)[nMuon-1] between -2.4 and 2.4
				and sort_array(Muon_Eta)[nMuon-2] between -2.4 and 2.4
				and sort_array(Muon_pfRelIso04_all)[nMuon-1] < 0.25
				and sort_array(Muon_pfRelIso04_all)[nMuon-2] < 0.25
				and sort_array(Muon_tightId)[nMuon-1] = 1
				and sort_array(Muon_tightId)[nMuon-2] = 1
			"""

di_mu_selected_df = sql(MuonSelectionQuery)

etime = time.time()

out_f = open("/home/sunyoung/nano/NanoAOD/TTHAnalyzer/DiMuonCutTest.txt","w")
result = "Muon-Cut test for a 'run2_2016MC_NANO_2.root' file : " + str(etime-stime) + " seconds"
out_f.write(result)

#di_mu_selected_df.createOrReplaceTempView("DiMu")
#di_mu_selected_df.cache()

