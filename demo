from digilog_n.PlasmaReader import PlasmaReader
from time import sleep

#pr = PlasmaReader('/tmp/plasma', 'RUL_RSLT', remove_after_reading=True)
pr = PlasmaReader('/tmp/plasma', 'PHM08')

while True:
    df = pr.to_pandas()
    if df is not None:
        print(df.info())
        #print(df.head())
    sleep(10)

