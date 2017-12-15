from __future__ import print_function
from __future__ import division
from root_pandas import read_root
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import DataType
from pyspark.sql import Row
import time
from pylab import * #shold be prior to the ROOT TTRee Object
import ROOT, os, getopt, sys, math, glob
from array import array
from root_pandas import *


f = ROOT.TFile("test.root", "recreate")

Mu_Pt = ROOT.std.vector('float')()

Nu_Mu = array('i',[0])

ALL = ROOT.TTree("nEvent", "nEvent")
ALL.Branch("Nu_Mu", Nu_Mu, "Nu_Mu/I")
ALL.Branch("Mu_Pt", Mu_Pt)


stime = time.time()

spark = SparkSession.builder \
        .appName("NanoAOD with Spark") \
        .config("spark.jars.packages","org.diana-hep:spark-root_2.11:0.1.11") \
        .getOrCreate()

sql = spark.sql

data_directory = "/home/nanoAOD/GluGlu_HToMuMu_M125_13TeV_powheg_pythia8/run2_2016MC_NANO_RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/171112_002433/0000/"


rcParams['figure.figsize'] = (12.0,8.0)
data_frame = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_1.root")

data_frame.createOrReplaceTempView("mc_file")
data_frame.cache()

small_sample = sql(""" SELECT * FROM mc_file LIMIT 2000 """)

small_sample.toPandas().to_root('/home/sunyoung/nano/NanoAOD/TTHAnalyzer/small_sample.root', key='Events')

f.Write()
f.Close()
