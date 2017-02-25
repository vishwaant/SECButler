# -*- coding: utf-8 -*-

import luigi
import os
import ujson
import string,requests
from bs4 import BeautifulSoup
from pprint import pprint

output_folder = "../../../Data/SEC"
forms_folder = output_folder+"/forms"
lookups_folder = output_folder+"/lookups"
URL="https://www.sec.gov/Archives/edgar/full-index/"

class PrepareEnv(luigi.Task):

    dirs = luigi.Parameter(description="Enter list of directories to be created followed by comma")

    def run(self):
        if not os.path.exists(output_folder): os.makedirs(output_folder)
        if not os.path.exists(lookups_folder): os.makedirs(lookups_folder)
        if not os.path.exists(forms_folder): os.makedirs(forms_folder)
        if self.dirs is not None:
            for dir in self.dirs.split(","):
                if not os.path.exists(forms_folder+"/"+dir): os.makedirs(forms_folder+"/"+dir)

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
        return [PrepareEnv(dirs=""),GetCrawler(self.date)]

    def slices(self,s, *args):
        position = 0
        for length in args:
            yield s[position:position + length]
            position += length

    def run(self):
        company_dict={}
        main_json ={'root':[]}
        forms = {}
        forminfo = {}
        crawl_file =  open(output_folder + "/crawler_{}.idx".format(self.date), "r")

        for line in crawl_file.readlines()[9:]:
            company,form,cik,dateoffile,url = self.slices(line,62,12,12,12,86)
            # cik_form_url_dict[cik.strip(),form.strip()]=[dateoffile.strip(),url.strip()]
            # if cik is not "" : company_dict[cik.strip()]=company.strip()
            if form.strip() not in forminfo:
                forminfo[form.strip()]=1
            else:
                forminfo[form.strip()]=forminfo[form.strip()]+1

            f = {
                'form' : form.strip(),
                'date' : dateoffile.strip(),
                'url' : url.strip()
            }

            if cik.strip() not in forms:
                forms[cik.strip()]=[f]
            else:
                forms[cik.strip()].append(f)

        main_json.get('root').append(forms)

        with open(output_folder+"/form_counts_{}.json".format(self.date),'w') as fm:
            ujson.dump(forminfo,fm,indent=4)
        with open(lookups_folder+"/company_{}.json".format(self.date),'w') as lkp:
            ujson.dump(company_dict,lkp,indent=4)
        with open(lookups_folder+"/cik_url_{}.json".format(self.date),'w') as urllkp:
            ujson.dump(main_json,urllkp,indent=4)


    def output(self):
        return [luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date)),
                luigi.LocalTarget(path=output_folder+"/form_counts_{}.json".format(self.date)),
                luigi.LocalTarget(path=lookups_folder+"/cik_url_{}.json".format(self.date))]



class ListCompanyURLByForm(luigi.Task):

    date = luigi.DateParameter()
    form = luigi.Parameter(default="8-K")

    def requires(self):
        return [PrepareEnv(dirs=""),GetCrawler(self.date),CreateCIKLookup(self.date)]

    def run(self):
        with open(lookups_folder+"/cik_url_{}.json".format(self.date),'r') as lkp:
            cik_form_url_dict = ujson.load(lkp)

            for item in cik_form_url_dict['root']:
                for k,v in item.iteritems():
                    if v[0]['form'] == self.form:
                        print k,v[0]['form'],v[0]['date'],v[0]['url']
                # print "++"

        # pprint(cik_form_url_dict)
        # for k,v in cik_form_url_dict.iteritems():
        #     print k.split(",")[1],v




class CreateParseConfig(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):
        return [PrepareEnv(dirs=""),GetCrawler(self.date)]

    def run(self):
        with open(lookups_folder+"/company_{}.json".format(self.date),'r') as lkp:
            print ujson.load(lkp)


    # def output(self):
    #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))



class Parse13FHR(luigi.Task):

    sec_root="https://www.sec.gov/"
    date = luigi.DateParameter()
    cik = luigi.Parameter()
    d_of_file = luigi.Parameter()
    url = luigi.Parameter()
    # url = "https://www.sec.gov/Archives/edgar/data/1602119/0000950123-17-000678-index.htm"

    def requires(self):
        return [PrepareEnv(dirs="13FHR"),GetCrawler(self.date),CreateCIKLookup(self.date)]

    def run(self):
        form_data = []
        sec_main_page = requests.get(self.url,timeout=60)
        sec_soup = BeautifulSoup(sec_main_page.text,"lxml")
        form13f_path = "".join([a["href"] for a in sec_soup.find_all('a') if a.text=="form13fInfoTable.html"])
        form13f_page = requests.get(self.sec_root+form13f_path,timeout=60)
        form13f_soup = BeautifulSoup(form13f_page.text, "lxml")
        table = form13f_soup.find('table',{"summary":"Form 13F-NT Header Information"})
        for tr in  table.find_all('tr'):
            form_data.append([td.text for td in tr.find_all('td')])
        print form_data

    def complete(self):
        return False

    # def output(self):
    #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))



    #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))


if __name__ == "__main__":
    luigi.run()
