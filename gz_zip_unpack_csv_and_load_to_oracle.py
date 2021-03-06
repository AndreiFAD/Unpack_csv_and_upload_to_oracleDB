#!/usr/bin/python
# -*- coding: cp1250  -*-
__author__ = 'Fekete Andr�s Demeter'

import glob
import shutil
import os
import csv
import subprocess
import cx_Oracle
import sys
import time
import zipfile
import gzip

class zip_unpack:

    def unpack_zip(self, file_name, outpath):

        fh = open(file_name, 'rb')
        z = zipfile.ZipFile(fh)
        for name in z.namelist():
            z.extract(name, outpath)

        fh.close()

class gz_unpack:

    def unpack_gz(self, file_name, outpath):

        for gzip_path in glob.glob(file_name):

            if os.path.isdir(gzip_path) == False:

                inF = gzip.open(gzip_path, 'rb')
                s = inF.read()
                inF.close()
                gzip_fname = os.path.basename(gzip_path)
                fname = gzip_fname[:-3]
                uncompressed_path = os.path.join(outpath, fname)
                open(uncompressed_path, 'wb').write(s)

class convertalas_():
    def convert_(self,inputpath,outpath):
        global _name
        try:

            files=glob.glob(inputpath+"/*")
            for name in files:

                    _name=name[len(inputpath)+1:]
                    try:

                         if _name[-3:]=="zip":

                              for file_name in glob.glob(inputpath+'/'+ _name):
                                  unpack=zip_unpack()
                                  unpack.unpack_zip(file_name, outpath)
                                  os.remove(file_name)

                         elif _name[-3:]==".gz":

                              for file_name in glob.glob(inputpath+'/'+ _name):
                                  unpack=gz_unpack()
                                  unpack.unpack_gz(file_name, outpath)
                                  os.remove(file_name)

                         elif _name[-3:]=="csv":
                              shutil.move(inputpath + "/" + _name, outpath + "/" + _name)

                         else:
                              os.remove(inputpath + "/" + _name)

                    except:
                        pass

        except:
            pass


class connection():

    def connct(self):

        global conn_db
        global connstrg
        global db
        global user
        global pwd
        global ip
        global port

        ip = 'host'
        port = port
        db='shema'
        user='user'
        pwd ='password'
        connstrg='user@\\"\\(description=\\(address=\\(host=host\\)\\(protocol=tcp\\)\\(port=port\\)\\)\\(connect_data=\\(sid=sid\\)\\)\\)\\"/password'

class report():

    def sql_report(self):

        global strsql
        global title2
        global table_name

        conndef=connection()
        conndef.connct()

        try:
            a ='select * from ' + table_name

            strsql =  a
            dsn_tns = cx_Oracle.makedsn(ip, port, db)
            conn = cx_Oracle.connect(user, pwd, dsn_tns)

            cur=conn.cursor()
            cur.execute(strsql)
            title2=[]
            for i in range(0, len(cur.description)):
                title2.append(cur.description[i][0])

            conn.close()

            title2=str(title2).strip('[]')
            title2=title2.replace("'","")
            title2=title2.replace(" ","")

        except:
            pass

class report_rownum():

    def sql_report_rownum(self):

        conndef=connection()
        conndef.connct()

        global ip
        global port
        global data_report_rownum

        try:

            strsql = 'Select count(1) from '+ table_name

            dsn_tns = cx_Oracle.makedsn(ip, port, db)
            conn = cx_Oracle.connect(user, pwd, dsn_tns)

            cur=conn.cursor()
            cur.execute(strsql)
            data_report_rownum = cur.fetchall()
            conn.commit()
            conn.close()

            data_report_rownum=str(data_report_rownum)
            data_report_rownum=str(data_report_rownum).strip('[]')
            data_report_rownum=data_report_rownum.replace(",","")
            data_report_rownum=data_report_rownum.replace("(","")
            data_report_rownum=data_report_rownum.replace(")","")
            data_report_rownum=str(data_report_rownum)

        except:
            pass

class csv_darabolas_to_sql_loader():

    global def_rownums
    global filename_csv_0
    global table_name
    global connstrg
    global real_upload_rows
    global read_rownum
    global row_count_infile

    def csv_darab_(self,input_path,filename_csv_0,table_name,file_name_load):

        csv.field_size_limit(sys.maxsize)

        conndef=connection()
        conndef.connct()

        run_report=report()
        run_report.sql_report()

        print('column: ',title2)

        def_rownums=1
        d='1'
        read_rownum=0
        row_count_infile=0
        real_upload_rows=""
        ttrrzz=filename_csv_0

        with open(input_path+"/"+filename_csv_0,'r') as cfile_:
            reader=csv.reader(cfile_,delimiter=';')
            row_count_infile = sum(1 for row in reader)

        with open(input_path+"/"+filename_csv_0,'r') as cfile_:
            reader=csv.reader(cfile_,delimiter=';')
            b=open("c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv", 'w' ,newline='')
            a=csv.writer(b,delimiter=";")

            for row in reader:

                        rowss=[]
                        for row_items in row:
                          rowss.append(row_items[:255])
                        if file_name_load=='1':
                            rowss.append(ttrrzz)
                        a.writerows([rowss])
                        def_rownums=def_rownums+1
                        read_rownum=read_rownum+1
                        if def_rownums < 60000:
                            if read_rownum==row_count_infile:
                                if d=='1':

                                    b.close()
                                    time.sleep(30)
                                    u=time.strftime('%H%M%S')
                                    g=open('c:\\Temp\\sqlload_csv_'+u+'.txt','w')
                                    g.write("options (skip = 1)\nload data\ninfile '" + "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv'" + "\nappend into table "+table_name+"\nfields terminated by ';' optionally enclosed by "+"'"+'"'+"'"+"\ntrailing nullcols\n(\n"+ title2 +"\n)")
                                    g.close()

                                    os.rename('c:\\Temp\\sqlload_csv_'+u+'.txt','c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                    gr=open('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','w')
                                    gr.write('sqlldr '+ connstrg +' control='+'c:\\Temp\\sqlload_csv_'+u+'.ctl'+' data='+ "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv"+' BAD='+"c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".bad"+ '\nexit')
                                    gr.close()

                                    os.rename('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','c:\\Temp\\sqlload_csv_bat_'+u+'.bat')
                                    pii = subprocess.Popen('c:\\Temp\\sqlload_csv_bat_'+u+'.bat',
                                                                   shell=False,
                                                                   stdin=subprocess.PIPE,
                                                                   stdout=subprocess.PIPE,
                                                                   stderr=subprocess.STDOUT,
                                                                   )

                                    pii_stdout, pii_err = pii.communicate()
                                    p_status = pii.wait()
                                    print('done - output: ', repr(pii_stdout),'- error: ',repr(pii_err),'status: ',p_status)

                                    os.remove('c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                    os.remove("c:\\Temp\\"+"/"+filename_csv_0[:-4]+"v"+d+".csv")
                                    os.remove('c:\\Temp\\sqlload_csv_bat_'+u+'.bat')

                                else:

                                    b.close()
                                    time.sleep(30)
                                    u=time.strftime('%H%M%S')
                                    g=open('c:\\Temp\\sqlload_csv_'+u+'.txt','w')
                                    g.write("options (skip = 0)\nload data\ninfile '" + "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv'" + "\nappend into table "+table_name+"\nfields terminated by ';' optionally enclosed by "+"'"+'"'+"'"+"\ntrailing nullcols\n(\n"+ title2 +"\n)")
                                    g.close()

                                    os.rename('c:\\Temp\\sqlload_csv_'+u+'.txt','c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                    gr=open('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','w')
                                    gr.write('sqlldr '+ connstrg +' control='+'c:\\Temp\\sqlload_csv_'+u+'.ctl'+' data='+ "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv"+' BAD='+"c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".bad"+ '\nexit')
                                    gr.close()

                                    os.rename('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','c:\\Temp\\sqlload_csv_bat_'+u+'.bat')
                                    pii = subprocess.Popen('c:\\Temp\\sqlload_csv_bat_'+u+'.bat',
                                                                   shell=False,
                                                                   stdin=subprocess.PIPE,
                                                                   stdout=subprocess.PIPE,
                                                                   stderr=subprocess.STDOUT,
                                                                   )

                                    pii_stdout, pii_err = pii.communicate()
                                    p_status = pii.wait()
                                    print('done - output: ', repr(pii_stdout),'- error: ',repr(pii_err),'status: ',p_status)

                                    os.remove('c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                    os.remove("c:\\Temp\\"+"/"+filename_csv_0[:-4]+"v"+d+".csv")
                                    os.remove('c:\\Temp\\sqlload_csv_bat_'+u+'.bat')

                                def_rownums=1
                            else:
                                pass

                        else:
                            if d=='1':

                                b.close()
                                time.sleep(30)
                                u=time.strftime('%H%M%S')
                                g=open('c:\\Temp\\sqlload_csv_'+u+'.txt','w')
                                g.write("options (skip = 1)\nload data\ninfile '" + "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv'" + "\nappend into table "+table_name+"\nfields terminated by ';' optionally enclosed by "+"'"+'"'+"'"+"\ntrailing nullcols\n(\n"+ title2 +"\n)")
                                g.close()

                                os.rename('c:\\Temp\\sqlload_csv_'+u+'.txt','c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                gr=open('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','w')
                                gr.write('sqlldr '+ connstrg +' control='+'c:\\Temp\\sqlload_csv_'+u+'.ctl'+' data='+ "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv"+' BAD='+"c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".bad"+ '\nexit')
                                gr.close()

                                os.rename('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','c:\\Temp\\sqlload_csv_bat_'+u+'.bat')
                                pii = subprocess.Popen('c:\\Temp\\sqlload_csv_bat_'+u+'.bat',
                                                               shell=False,
                                                               stdin=subprocess.PIPE,
                                                               stdout=subprocess.PIPE,
                                                               stderr=subprocess.STDOUT,
                                                               )

                                pii_stdout, pii_err = pii.communicate()
                                p_status = pii.wait()
                                print('done - output: ', repr(pii_stdout),'- error: ',repr(pii_err),'status: ',p_status)

                                os.remove('c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                os.remove("c:\\Temp\\"+"/"+filename_csv_0[:-4]+"v"+d+".csv")
                                os.remove('c:\\Temp\\sqlload_csv_bat_'+u+'.bat')

                            else:

                                b.close()
                                time.sleep(30)
                                u=time.strftime('%H%M%S')
                                g=open('c:\\Temp\\sqlload_csv_'+u+'.txt','w')
                                g.write("options (skip = 0)\nload data\ninfile '" + "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv'" + "\nappend into table "+table_name+"\nfields terminated by ';' optionally enclosed by "+"'"+'"'+"'"+"\ntrailing nullcols\n(\n"+ title2 +"\n)")
                                g.close()

                                os.rename('c:\\Temp\\sqlload_csv_'+u+'.txt','c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                gr=open('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','w')
                                gr.write('sqlldr '+ connstrg +' control='+'c:\\Temp\\sqlload_csv_'+u+'.ctl'+' data='+ "c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv"+' BAD='+"c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".bad"+ '\nexit')
                                gr.close()

                                os.rename('c:\\Temp\\sqlload_csv_bat_'+u+'.txt','c:\\Temp\\sqlload_csv_bat_'+u+'.bat')
                                pii = subprocess.Popen('c:\\Temp\\sqlload_csv_bat_'+u+'.bat',
                                                               shell=False,
                                                               stdin=subprocess.PIPE,
                                                               stdout=subprocess.PIPE,
                                                               stderr=subprocess.STDOUT,
                                                               )

                                pii_stdout, pii_err = pii.communicate()
                                p_status = pii.wait()
                                print('done - output: ', repr(pii_stdout),'- error: ',repr(pii_err),'status: ',p_status)

                                os.remove('c:\\Temp\\sqlload_csv_'+u+'.ctl')
                                os.remove("c:\\Temp\\"+"/"+filename_csv_0[:-4]+"v"+d+".csv")
                                os.remove('c:\\Temp\\sqlload_csv_bat_'+u+'.bat')

                            i=int(d)+1
                            d=str(i)

                            b=open("c:\\Temp\\"+filename_csv_0[:-4]+"v"+d+".csv", 'w',newline='')
                            a=csv.writer(b,delimiter=';')
                            def_rownums=1

        selections=report_rownum()
        selections.sql_report_rownum()

        real_upload_rows=data_report_rownum
        real_upload_rows=str(real_upload_rows)

        read_rownum=str(read_rownum)

        print('Finish uploading -- '+read_rownum+' rows from -- '+filename_csv_0+' file  to -- '+table_name+' table\n-->>|<<-- <<>> Uploaded rows: '+ real_upload_rows + ' <<>> -->>|<<--')

if __name__ == "__main__":


    input_path = ""
    filename_csv_0 = ""
    table_name = ""
    file_name_load = ""

    start=csv_darabolas_to_sql_loader()
    start.csv_darab_(input_path, filename_csv_0, table_name, file_name_load)