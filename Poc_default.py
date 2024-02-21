import copy
import json
import avro
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

with open("schema_file.txt","r") as f:
    avro_schema_literal = f.readlines()
    app_string = '"null"'
    def_string = '"default" : null'
    false_list = ["record", "{", "null", "array", "name"]
    i = 0
    length_list = len(avro_schema_literal)
    while (i < length_list):
        x = avro_schema_literal[i]
        fid = x.find("type")
        if fid != -1:
            #fid1 = x.find("record")
            #if fid1 == -1:
            if any(y in x for y in false_list):
                print("This line doesnt need an edit")
            else:
                #fid2 = x.find("{")
                #if fid2 == -1:
                fid3 = x.find(":")
                if fid3 != -1:
                    final = x[:(fid3+1)] + ' [' +  app_string  + ',' + x[(fid3 + 1):] + ']' + ',' + def_string
                    print(final)
                    avro_schema_literal[i] = final
                else:
                    print("fid3 loop out")
        i += 1
    #print(avro_schema_literal)
    with open("schema_file1.txt","w") as wr:
        wr.writelines(avro_schema_literal)
    print("Job completed successfully")