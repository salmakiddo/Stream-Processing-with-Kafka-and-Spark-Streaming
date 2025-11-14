from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.compute import Server
from diagrams.programming.framework import Flask
from diagrams.onprem.analytics import Spark

with Diagram("IoT Streaming Architecture", show=False, direction="LR"):
    with Cluster("Producers"):
        producers = Server("IoT Producers\n4-18 threads\n2,429 msg/s peak")
    
    with Cluster("Kafka Cluster"):
        kafka = Kafka("iot-sensors topic\n3 partitions\n246K+ messages")
    
    with Cluster("Spark Streaming"):
        spark = Spark("Structured Streaming\n603-604 msg/s\n1-1.5s batches")
    
    with Cluster("Dashboard"):
        dashboard = Flask("Flask Server\nlocalhost:5000\n5s refresh")
    
    producers >> Edge(label="JSON messages") >> kafka
    kafka >> Edge(label="Consumer API") >> spark
    spark >> Edge(label="SQL queries") >> dashboard