from digilog_n.PlasmaReader import PlasmaReader

pr = PlasmaReader('/tmp/plasma', 'RUL_RSLT', remove_after_reading=True)
df = pr.to_pandas()

pr = PlasmaReader('/tmp/plasma', 'NOTIFY', remove_after_reading=True)
df = pr.to_pandas()
