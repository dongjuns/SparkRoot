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

f = ROOT.TFile("test.root", "recreate")
#Dilep = ROOT.TLorentzVector()
#Mu1 = ROOT.TLorentzVector()
#Mu2 = ROOT.TLorentzVector()

Mu_Pt = ROOT.std.vector('float')()
#Mu_Eta = ROOT.std.vector('float')()
#Mu_Charge = ROOT.std.vector('float')()
#Mu_Phi = ROOT.std.vector('float')()
#Mu_M = ROOT.std.vector('float')()

#El_Pt = ROOT.std.vector('float')()
#El_Eta = ROOT.std.vector('float')()

#Jet_Pt = ROOT.std.vector('float')()
#Jet_Eta = ROOT.std.vector('float')()
#Jet_CSVV2 = ROOT.std.vector('float')()

#Event_No = array("i",[0])
Nu_Mu = array('i',[0])
#Nu_El = array("i",[0])
#Nu_Jet = array("i",[0])
#Nu_BJet = array("i",[0])
#Nu_NonBJet = array("i",[0])


ALL = ROOT.TTree("nEvent", "nEvent")
#ALL.Branch("Event_No", Event_No, "Event_No/I")
#ALL.Branch("Dilep", "TLorentzVector", Dilep)
#ALL.Branch("Mu1", "TLorentzVector", Mu1)
#ALL.Branch("Mu2", "TLorentzVector", Mu2)
ALL.Branch("Nu_Mu", Nu_Mu, "Nu_Mu/I")
ALL.Branch("Mu_Pt", Mu_Pt)
#ALL.Branch("Mu_Eta", Mu_Eta)
#ALL.Branch("Nu_El", Nu_El, "Nu_El/I")
#ALL.Branch("El_Pt", El_Pt)
#ALL.Branch("El_Eta", El_Eta)
#ALL.Branch("Nu_Jet", Nu_Jet, "Nu_Jet/I")
#ALL.Branch("Jet_Pt", Jet_Pt)
#ALL.Branch("Jet_Eta", Jet_Eta)
#ALL.Branch("Nu_BJet", Nu_BJet, "Nu_BJet/I")


stime = time.time()

spark = SparkSession.builder \
        .appName("NanoAOD with Spark") \
        .config("spark.jars.packages","org.diana-hep:spark-root_2.11:0.1.11") \
        .getOrCreate()

sql = spark.sql

data_directory = "/home/nanoAOD/GluGlu_HToMuMu_M125_13TeV_powheg_pythia8/run2_2016MC_NANO_RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/171112_002433/0000/"


rcParams['figure.figsize'] = (12.0,8.0)
data_frame = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_1.root")

for i in range(2,3):
    temp_df = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_"+str(i)+".root")
    data_frame = data_frame.union(temp_df)

nMu = data_frame.rdd.filter(lambda x : x['nMuon'] <2) \
			.count()

#data_frame = spark.read.format("org.dianahep.sparkroot").load(data_directory+"run2_2016MC_NANO_1.root")
### number of Muon ###
#nMu = data_frame.rdd.flatMap(lambda x: [x['nMuon']]).collect()
#for i in range(len(nMu)):
#    Nu_Mu[0] = nMu[i]
#    ALL.Fill()
######################

#def testfunction(x):
#    a = dict()
#    a['nMuon'] = x['nMuon']
#    return Row(a)
#
#nMu = data_frame.rdd.flatMap(lambda x : testfunction(x))
#nMu = data_frame.rdd.flatMap(lambda x : [x['nMuon']]).collect()


def MuonSelection (event):
    Mu_pt, Mu_eta, Mu_iso, Mu_phi =  [], [], [], []
    for i in range(event['nMuon']):
        if event['Muon_pt'] < 20 \
           and abs(event['Muon_eta']) < 2.4 \
	   and  event['Muon_pfRelIso04_all'] < 0.25 \
	   and event['Muon_tightId'] == True:
               Mu_pt.append(event['Muon_pt'])
               Mu_eta.append(event['Muon_eta'])
               Mu_iso.append(event['Muon_pfRelIso04_all'])
    result = dict()
    result['nMuon']=len(Mu_pt)
    result['Muon_pt'] = Mu_pt
    result['Muon_eta'] = Mu_eta
    result['Muon_iso'] = Mu_iso
    return Row(result)

Muon_Cut_Rdd = data_frame.rdd.flatMap(lambda x : MuonSelection(x))

DiMuon_df = Muon_Cut_Rdd.toDF().createOrReplaceTempView("NanoAOD")
DiMuon_df.cache()

Muon_Cut_DF = sql("""select Muon_pt from NanoAOD """)
Muon_pt = Muon_Cut_DF.rdd.collect()

print("Muon_pt : ", Muon_pt[1:10])
for i in range(len(Muon_pt)):
    Mu_Pt.clear()
    Mu_Pt.push_back(Muon_pt[i])
    ALL.Fill()

#print(" # : ", nMu)
#print(" # : ", nMu2)

f.Write()
f.Close()
