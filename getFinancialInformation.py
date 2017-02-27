# -*- coding: utf-8 -*-

import os
import re
import ujson

import luigi
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



class ListCompanyURLByForm(luigi.Task):

    date = luigi.DateParameter()
    form = luigi.Parameter(default="8-K")

    def requires(self):
        return [PrepareEnv(dirs=self.form),GetCrawler(self.date),CreateCIKLookup(self.date)]

    def run(self):
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
        # print processed_param_list
        for elem in param_list:
        #     # print elem
            if elem[-4] in processed_param_list:
                param_list.remove(elem)
        #     # else:
        #     #     print "N",elem
        #
        # print param_list.__len__()

        yield Parse13FHR(date=self.date,list=param_list)
        # print param_list

    # def output(self):
    #     return luigi.LocalTarget(path=forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(self.form,format(self.date),format(cik),format(d_of_file)))



class Parse13FHR(luigi.Task):

    sec_root="https://www.sec.gov/"
    form = '13F-HR'
    list = luigi.ListParameter()
    date = luigi.DateParameter()
    # cik = luigi.Parameter()
    # d_of_file = luigi.Parameter()
    # url = luigi.Parameter()
    # url = "https://www.sec.gov/Archives/edgar/data/1602119/0000950123-17-000678-index.htm"

    def requires(self):
        return [PrepareEnv(dirs="13F-HR"),GetCrawler(self.date),CreateCIKLookup(self.date)]

    def run(self):
        print "length ",self.list.__len__()
        for config in self.list:
            cik = config[0]
            form = config[1]
            d_of_file = config[2]
            url = config[3]
            print "Parsing 13F-HR"
            print cik,form,d_of_file,url
            form_data = []
            sec_main_page = requests.get(url,timeout=60)
            sec_soup = BeautifulSoup(sec_main_page.text,"lxml")
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
            with open(forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(form,format(self.date),format(cik),format(d_of_file)),'w') as dta:
                ujson.dump(form_data,dta)
            print "Completed Parsing"

    def output(self):
        return [luigi.LocalTarget(path=forms_folder+"/"+self.form+"/%s_%s_%s_%s.json"%(self.form,format(self.date),format(config[0]),format(config[2]))) for config in self.list]
        # cik = config[0]
        # form = config[1]
        # d_of_file = config[2]
        # url = config[3])


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
