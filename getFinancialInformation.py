# -*- coding: utf-8 -*-
"""

Author: Vishwaant Kannaiyan
Notes: This Program consumes SEC forms filed by companies and parses them to extract
    key information about their performance and revenue.
The current version primarily focuses on Form 13F-HR which is submitted by Institutional investors

"""

import os
import re
import ujson
from datetime import date

import luigi
import pandas as pd
import requests
from bs4 import BeautifulSoup

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



class ListCompanyURLByForm(luigi.WrapperTask):

    date = luigi.DateParameter()
    form = luigi.Parameter(default="8-K")

    def requires(self):
        param_list=[]
        processed_param_list = []
        for file_name in os.listdir(forms_folder + "/" + self.form):
            # 13F-HR_2017-02-20_1041241_2017-02-08.json
            fl_split = file_name.replace('.json','').split("_")
            form_name = unicode(fl_split[0], "utf-8")
            cik_name = unicode(fl_split[2], "utf-8")
            date_file = unicode(fl_split[3], "utf-8")
            processed_param_list.append(cik_name)
            # processed_param_list.append([cik_name, form_name, date_file])

        with open(lookups_folder+"/cik_url_{}.json".format(self.date),'r') as lkp:
            cik_form_url_dict = ujson.load(lkp)
            for item in cik_form_url_dict['root']:
                for k,v in item.iteritems():
                    if v[0]['form'] == self.form:
                        param_list.append([k,v[0]['form'],v[0]['date'],v[0]['url']])
                        # print k,v[0]['form'],v[0]['date'],v[0]['url']

        # print processed_param_list.__len__()
        # # print processed_param_list
        # for elem in param_list:
        # #     # print elem
        #     if elem[0] in processed_param_list:
        #         param_list.remove(elem)
        # #     # else:
        # #     #     print "N",elem
        # #
        # print param_list.__len__()

        for params in param_list:
            yield Parse13FHR(date=self.date,config=params)
        # print param_list

    # def output(self):
    #     return luigi.LocalTarget(path=forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(self.form,format(self.date),format(cik),format(d_of_file)))



class LoadToDataFrame(luigi.Task):
    form = luigi.Parameter(default='13F-HR')
    rundate = luigi.DateParameter(default=date.today().strftime("%Y-%m-%d"))

    def returnDataFrame(self,FILE_N):
        file_parts = FILE_N.replace(".json", "").split("_")
        with open(forms_folder+"/"+self.form+"/"+FILE_N) as json_file:
            dumps = ujson.load(json_file)

        df = pd.DataFrame(dumps)
        ##Drop the first three rows
        df.columns = ["NAME OF ISSUER", "TITLE OF CLASS", "CUSIP", "VALUE", "SHARES(X$1000)", "SH/PRN AMT", "PUT/CALL",
                      "INVESTMENT DISCRETION", "OTHER MANAGER", "VOTING AUTH SOLE", "VOTING AUTH SHARED",
                      "VOTING AUTH NONE"]
        df.insert(0, "CIK", file_parts[2])
        df.insert(1, "FILE DATE", file_parts[3])
        df.drop(df.index[[0, 1, 2]], inplace=True)
        df.drop(["TITLE OF CLASS", "SH/PRN AMT", "PUT/CALL",
                      "INVESTMENT DISCRETION", "OTHER MANAGER", "VOTING AUTH SOLE", "VOTING AUTH SHARED",
                      "VOTING AUTH NONE"],axis=1, inplace=True)
        return df


    def run(self):
        df_full = pd.DataFrame()
        for file_name in os.listdir(forms_folder + "/" + self.form):
            print "Extacting from ",file_name
            df = self.returnDataFrame(file_name)
            df_full = pd.concat([df,df_full])

        df_full.to_pickle(lookups_folder+"/"+self.form+".p")

    def output(self):
        return luigi.LocalTarget(path=lookups_folder+"/"+self.form+".p")



class Parse13FHR(luigi.Task):

    sec_root="https://www.sec.gov/"
    form = '13F-HR'
    config = luigi.ListParameter()
    date = luigi.DateParameter()
    # cik = luigi.Parameter()
    # d_of_file = luigi.Parameter()
    # url = luigi.Parameter()
    # url = "https://www.sec.gov/Archives/edgar/data/1602119/0000950123-17-000678-index.htm"

    def requires(self):
        return [PrepareEnv(dirs="13F-HR")]

    def run(self):
        # print "length ",self.list.__len__()
        # for config in self.list:
        cik = self.config[0]
        form = self.config[1]
        d_of_file = self.config[2]
        url = self.config[3]
        print "Parsing 13F-HR"
        print cik,form,d_of_file,url
        form_data = []
        sec_main_page = requests.get(url,timeout=120)
        sec_soup = BeautifulSoup(sec_main_page.text,"lxml")
        try:
            form13f_path = "".join([a["href"] for a in sec_soup.find_all('a') if re.match(r'.*\.(htm|html)$', a.text) and a.text not in 'primary_doc.html'])
            print form13f_path

            form13f_page = requests.get(self.sec_root+form13f_path,timeout=60)
            # print form13f_page.status_code
            form13f_soup = BeautifulSoup(form13f_page.text, "lxml")
            # print form13f_soup
            table = form13f_soup.find('table',{"summary":"Form 13F-NT Header Information"})
            # print table
            for tr in  table.find_all('tr'):
                form_data.append([td.text for td in tr.find_all('td')])
        except:
            print "Some issue with ",url

        with open(forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(form,format(self.date),format(cik),format(d_of_file)),'w') as dta:
            ujson.dump(form_data,dta)
        print "Completed Parsing"

    def output(self):
        return [luigi.LocalTarget(path=forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(self.form,format(self.date),format(self.config[0]),format(self.config[2])))]



    class CreateParseConfig(luigi.Task):
        date = luigi.DateParameter()

        def requires(self):
            return [PrepareEnv(dirs=""), GetCrawler(self.date)]

        def run(self):
            with open(lookups_folder + "/company_{}.json".format(self.date), 'r') as lkp:
                print ujson.load(lkp)

        # def output(self):
        #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))
        def complete(self):
            return False



                #     return luigi.LocalTarget(path=lookups_folder+"/company_{}.json".format(self.date))


if __name__ == "__main__":
    luigi.run()
