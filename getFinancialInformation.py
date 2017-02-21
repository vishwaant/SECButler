# -*- coding: utf-8 -*-

import luigi
import time,sys,json,os
import datetime,httplib,urllib2
import string,requests
from bs4 import BeautifulSoup
import pickle
import struct

output_folder = "../../../Data/SEC"
lookups_folder = output_folder+"/lookups"
URL="https://www.sec.gov/Archives/edgar/full-index/"

class PrepareEnv(luigi.Task):

    def run(self):
        if not os.path.exists(output_folder): os.makedirs(output_folder)
        if not os.path.exists(lookups_folder): os.makedirs(lookups_folder)

    def output(self):
        return luigi.LocalTarget(path=output_folder)


class getCrawler(luigi.Task):

    date = luigi.DateParameter()

    def run(self):
        CRAWLER_URL = URL+"crawler.idx"
        response = requests.get(CRAWLER_URL)
        with open(output_folder+"/crawler_{}.idx".format(self.date),"wb") as crawl:
            crawl.write(response.text)


    def output(self):
        return luigi.LocalTarget(path=output_folder+"/crawler_{}.idx".format(self.date))

class createCIKLookup(luigi.Task):

    date = luigi.DateParameter()

    def slices(self,s, *args):
        position = 0
        for length in args:
            yield s[position:position + length]
            position += length

    def run(self):
        print "++++++++++++"
        crawl_file =  open(output_folder + "/crawler_{}.idx".format(self.date), "r")
        for line in crawl_file.readlines():
            print list(self.slices(line,62,12,12,98))
        print "++++++++++++"

    # def output(self):
    #     return luigi.LocalTarget(path=output_folder + "/crawler_{}.idx".format(self.date))




if __name__ == "__main__":
    luigi.run()
