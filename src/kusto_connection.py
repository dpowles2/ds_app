from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from enum import StrEnum


class Clusters(StrEnum): 
    prod = "https://btmorchestrationprod.australiasoutheast.kusto.windows.net/"
    shared = "https://btmorchestrationshared.australiasoutheast.kusto.windows.net"
    dev = "https://neoadxdev.australiasoutheast.kusto.windows.net"



class Kusto_Connection():
    def __init__(self, cluster = Clusters.shared):
        self.cluster = cluster
        
        builder = KustoConnectionStringBuilder.with_interactive_login(cluster)
        self.kusto_client = KustoClient(builder)

    def query(self, db, query):
        response = self.kusto_client.execute(db, query)
        return dataframe_from_result_table(response.primary_results[0])

    def __call__(self, db, query):
        return self.query(db=db,query=query)