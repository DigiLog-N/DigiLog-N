from PlasmaReader import PlasmaReader
from DataSourceRegistry import DataSourceRegistry
from time import sleep
from NotifyWriter import NotifyWriter




def main():
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    # TODO: change this is match toy results from spark. for tright now
    # it doesn't matter.
    data_source = dsr.get_data_source('DigiLog-N Notifications')

    if not data_source:
        print("Error: Could not locate Notifications data-source.")
        exit(1)

    pr = PlasmaReader(data_source.get_path_to_plasma_file(), 'SPARK_RSLT', remove_after_reading=True)
    nw = NotifyWriter(data_source.get_path_to_plasma_file())

    while True:
        print("Looking at data....")
        pdf = pr.to_pandas()
        if pdf is None:
            print("No new results from spark")
        else:
            print("New results from spark!")
            print("Notifying the right people...")
            nw.write(['unique.identifier@gmail.com', 'ucsdboy@gmail.com'], 'We have a new result from Spark.', 'new results from spark')

        print("sleeping 10 seconds...")
        # sleep an arbitrary amount before checking for more notifications 
        sleep(10)

if __name__ == '__main__':
    main()
