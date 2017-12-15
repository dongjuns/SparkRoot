from __future__ import print_function
from __future__ import division

from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("LHCb Open Data with Spark") \
        .config("spark.jars.packages","org.diana-hep:spark-root_2.11:0.1.11")\
        .getOrCreate()

sql = spark.sql
#sql("select 'Hello World!'").show()

data_directory = "/home/sunyoung/nano/NanoAOD/TTHAnalyzer/data/"

from pylab import *
rcParams['figure.figsize'] = (12.0,8.0)

sim_data_df = spark.read.format("org.dianahep.sparkroot").load(data_directory+"PhaseSpaceSimulation.root")
#sim_data_df = sim_data_format.load("test.txt")
sim_data_df.createOrReplaceTempView("sim_data")
sim_data_df.cache()
a = sim_data_df.count()

f = open("out.txt","w")


b = sim_data_df.printSchema()
h1px_data = sim_data_df.select("H1_PX").toPandas() # select H1_PX data and moves it to Pandas
h1px_data.plot.hist(bins=31, range=[-150000, 150000], title="Histogram - distribution of H1_PX, simulation data")
xlabel('H1_PX (MeV/c)')
ylabel('Count')
show()

histogram_h1px_df = sql("""
     select round(H1_PX/10000,0) * 10000 as bin, count(1) as count
     from sim_data 
     group by round(H1_PX/10000,0) order by 1
     """)
histogram_h1px_pandas = histogram_h1px_df.toPandas()
histogram_h1px_pandas.plot.bar(x='bin', y='count', title="Histogram - distribution of H1_PX, simulation data,")
xlabel('H1_PX (MeV/c)')
ylabel('Count');
show()



sql("""
     select round(H1_PX/10000,0) * 10000 as bin, count(1) as count
     from sim_data 
     group by round(H1_PX/10000,0) order by 1
     """).show(50)


p_tot = sql("""
    select H1_PX, H1_PY, H1_PZ, round(sqrt(H1_PX*H1_PX + H1_PY*H1_PY + H1_PZ*H1_PZ),2) H1_PTOT  
    from sim_data 
    where H1_PROBK = 1.0""")

p_tot.show(5)

h1ptot_data_plot = p_tot.select("H1_PTOT").toPandas().plot.hist(bins=31, range=[0, 550000]) 
xlabel('H1_PTOT (MeV/c)')
ylabel('Count');


