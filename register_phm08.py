import pymongo
import json
from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry


if __name__ == '__main__':
    mongo_client = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    ds = DataSource('PHM08 Prognostics Data Challenge Dataset')
    ds.set_path_to_plasma_file('/tmp/plasma')
    ds.set_description('This dataset was used for the prognostics challenge competition at the International Conference on Prognostics and Health Management (PHM08). The challenge is still open for the researchers to develop and compare their efforts against the winners of the challenge in 2008. References to the three winner papers are provided below.\n \n [1] Heimes, F.O., “Recurrent neural networks for remaining useful life estimation”, in the Proceedings of the 1st International Conference on Prognostics and Health Management (PHM08), Denver CO, Oct 2008.\n [2] Tianyi Wang, Jianbo Yu,  Siegel, D.,  Lee, J., “A similarity-based prognostics approach for Remaining Useful Life estimation of engineered systems”, in the Proceedings of the 1st International Conference on Prognostics and Health Management (PHM08), Denver CO, Oct 2008.\n [3] Peel, L., “Recurrent neural networks for remaining useful life estimation”, in the Proceedings of the 1st International Conference on Prognostics and Health Management (PHM08), Denver CO, Oct 2008.\n \n Experimental Scenario\n Data sets consist of multiple multivariate time series. Each data set is further divided into training and test subsets. Each time series is from a different engine – i.e., the data can be considered to be from a fleet of engines of the same type. Each engine starts with different degrees of initial wear and manufacturing variation which is unknown to the user. This wear and variation is considered normal, i.e., it is not considered a fault condition. There are three operational settings that have a substantial effect on engine performance. These settings are also included in the data. The data are contaminated with sensor noise.\n The engine is operating normally at the start of each time series, and starts to degrade at some point during the series. In the training set, the degradation grows in magnitude until a predefined threshold is reached beyond which it is not preferable to operate the engine. In the test set, the time series ends some time prior to complete degradation. The objective of the competition is to predict the number of remaining operational cycles before in the test set, i.e., the number of operational cycles after the last cycle that the engine will continue to operate properly.\n')
    ds.set_provider('International Conference on Prognostics and Health Management')
    #name, units, precision, description, data_type
    ds.add_field('unit', None, None, 'engine unit', 'int')
    ds.add_field('cycle', None, None, 'one cycle is assumed to be one take-off + flight-time + one landing', 'int')
    ds.add_field('op1', 'unknown', 4, 'unknown', 'float')
    ds.add_field('op2', 'unknown', 4, 'unknown', 'float')
    ds.add_field('op3', 'unknown', 1, 'unknown', 'float')
    ds.add_field('sensor01', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor02', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor03', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor04', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor05', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor06', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor07', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor08', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor09', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor10', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor11', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor12', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor13', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor14', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor15', 'unknown', 4, 'unknown', 'float')
    ds.add_field('sensor16', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor17', 'unknown', 0, 'unknown', 'float')
    ds.add_field('sensor18', 'unknown', 0, 'unknown', 'float')
    ds.add_field('sensor19', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor20', 'unknown', 2, 'unknown', 'float')
    ds.add_field('sensor21', 'unknown', 4, 'unknown', 'float')

    mongo_client.add_data_source(ds)

    data_sources = mongo_client.get_data_sources()

    for item in data_sources:
        print(item.json())
        #mongo_client.remove_data_source(item)
