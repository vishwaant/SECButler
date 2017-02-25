# -*- coding: utf-8 -*-

import luigi
import os
import ujson
import string,requests
from bs4 import BeautifulSoup

output_folder = "../../../Data/SEC"
forms_folder = output_folder+"/forms"
lookups_folder = output_folder+"/lookups"
URL="https://www.sec.gov/Archives/edgar/full-index/"

class PrepareEnv(luigi.Task):

    def run(self):
        if not os.path.exists(output_folder): os.makedirs(output_folder)
        if not os.path.exists(lookups_folder): os.makedirs(lookups_folder)
        if not os.path.exists(forms_folder): os.makedirs(forms_folder)

    def output(self):
        return [luigi.LocalTarget(path=output_folder),luigi.LocalTarget(path=lookups_folder),luigi.LocalTarget(path=forms_folder)]


class GetCrawler(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):
        return PrepareEnv()

    def run(self):
        CRAWLER_URL = URL+"crawler.idx"
        response = requests.get(CRAWLER_URL)
        with open(output_folder+"/crawler_{}.idx".format(self.date),"wb") as crawl:
            crawl.write(response.text)


    def output(self):
        return luigi.LocalTarget(path=output_folder+"/crawler_{}.idx".format(self.date))



class CreateCIKLookup(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):
        return [PrepareEnv(),GetCrawler(self.date)]

    def slices(self,s, *args):
        position = 0
        for length in args:
            yield s[position:position + length]
            position += length

    def run(self):
        company_dict={}
        forminfo = {}
        crawl_file =  open(output_folder + "/crawler_{}.idx".format(self.date), "r")

        for line in crawl_file.readlines()[9:]:
            company,form,cik,dateoffile,url = self.slices(line,62,12,12,12,86)
            if cik is not "" : company_dict[cik.strip()]=company.strip()
            if form.strip() not in forminfo:
                forminfo[form.strip()]=1
            else:
                forminfo[form.strip()]=forminfo[form.strip()]+1

        with open(output_folder+"/form_counts_{}.json".format(self.date),'w') as fm:
            ujson.dump(forminfo,fm,indent=4)

        with open(lookups_folder+"/company_{}.json".format(self.date),'w') as lkp:
            ujson.dump(company_dict,lkp,indent=4)

    def output(self):
        return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))


class parse13FHR(luigi.Task):

    date = luigi.DateParameter()
    sec_root="https://www.sec.gov/"
    url = "https://www.sec.gov/Archives/edgar/data/1602119/0000950123-17-000678-index.htm"

    def requires(self):
        return [PrepareEnv(),GetCrawler(self.date),CreateCIKLookup(self.date)]


    def run(self):
        sec_main_page = requests.get(self.url,timeout=60)
        sec_soup = BeautifulSoup(sec_main_page.text,"lxml")
        form13f_path = "".join([a["href"] for a in sec_soup.find_all('a') if a.text=="form13fInfoTable.html"])
        form13f_page = requests.get(self.sec_root+form13f_path,timeout=60)
        form13f_soup = BeautifulSoup(form13f_page.text, "lxml")
        print form13f_soup

    def complete(self):
        return False

    # def output(self):
    #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))



    #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))


if __name__ == "__main__":
    luigi.run()
