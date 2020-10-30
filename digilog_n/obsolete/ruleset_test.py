#!/usr/bin/env python

# This code developed from a demo script described in durable rules docs.
# TODO: Locate reference
import logging
from os import environ, path, makedirs, getcwd
from argparse import ArgumentParser
from digilog_n.PlasmaReader import PlasmaReader
from digilog_n.DataSourceRegistry import DataSourceRegistry
from time import sleep
from digilog_n.NotifyWriter import NotifyWriter
from durable.lang import *


with ruleset('engine'):
    @when_all(m.estimated_rul >= 10)
    def maint_fine(c):
        # durable rules library needs a rule to handle the facts that don't qualify under the rule below.
        pass

    @when_all(m.estimated_rul < 10)
    def maint_warning(c):
        print("Send a notification to the maintainance crew. Unit %s has an estimated rul of %d." % (c.m.unit, c.m.estimated_rul))
        c.s.mycount += 1
        c.s.mybool = True

    @when_all((m.estimated_rul < 20) & (m.unit == 'Kermit'))
    def maint_warning(c):
        msg = "Send a notification to the maintainance crew. Special Unit %s has an estimated rul of %d." % (c.m.unit, c.m.estimated_rul))
        c.s.mycount += 1
        c.s.mybool = True
        nw.write(['unique.identifier@gmail.com', 'charlie@canvasslabs.com'], msg, 'test message2')

    @when_all((s.mycount > 2) & (s.mybool == True))
    def maint_warning2(c):
        msg = "Send a notification to the logistics team. Order more parts, 2 or more units are old!"
        # mybool is to prevent the rule from firing forever.
        # mybool should only be set to true when the count has been incremented,
        #  and the rule needs to be re-evaluated. Otherwise, the message should
        #  be considered sent.
        c.s.mybool = False
        nw.write(['unique.identifier@gmail.com', 'charlie@canvasslabs.com'], msg, 'test message')



def main():
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')
    update_state('engine', { 'mycount': 0 , 'mybool': False})

    # TODO: Change this to match results from Spark. For right now, it doesn't matter.
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
            #print("Notifying the right people...")
            # initialize state
            assert_fact('engine', { 'unit': 'Kermit', 'estimated_rul': 10 })
            assert_fact('engine', { 'unit': 'Kermit', 'estimated_rul': 100 })
            assert_fact('engine', { 'unit': 'Greedy', 'estimated_rul': 50 })
            assert_fact('engine', { 'unit': 'Greedy', 'estimated_rul': 52 })
            assert_fact('engine', { 'unit': 'Tweety', 'estimated_rul': 9 })
            assert_fact('engine', { 'unit': 'Testy', 'estimated_rul': 9 })


        print("sleeping 10 seconds...")
        # sleep an arbitrary amount before checking for more notifications 
        sleep(10)

if __name__ == '__main__':
    main()

