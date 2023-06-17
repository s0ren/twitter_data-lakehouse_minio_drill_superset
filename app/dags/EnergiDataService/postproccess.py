def collect_jsons(path = './' +  r'.\app\dags\EnergiDataService\data\back'):
    L = {}
    from glob import glob
    # path = './' +  r'.\app\dags\EnergiDataService\data'
    files = glob(path + r'\*#1.json')

    import re
    file_pattern = r'^((.+)_#(\d+)(.+))$'

    for filename in files:
        # print(filename)
        m = re.search(file_pattern, filename)
        if m:
            g = m.groups()
            # print(m.groups())
            # print(g[1] + '*' + g[3])
            files = glob( g[1] + '*' + g[3])
            # print(*files, sep='\n\t')
            # print(files)
            L[ g[1] + '.json' ] =  files
            
    return L

def merge_jsons(path='./', collections = {'./datafile.json': ['./datafile_#1.json', './datafile_#2.json', './datafile_#3.json']}):
    import json
    L = []
    for outfile, infiles in collections.items():
        records = []               # to hold acumulated records
        for filename in infiles:
            with open(filename, 'r') as f:
                j = json.load(f)
            records.extend(j['records'])
            print('.', end='')

        orig_path = outfile.rpartition('\\')[0]
        outfile = outfile.replace(orig_path, path)
        
        with open(outfile, 'w+') as f:
            json.dump(records, f)
        L.append(outfile)
    return L

def add_year_month_day(json_falepathname_list = ['./datafiles/datafile1.json', './datafiles/datafile2.json', './datafiles/datafile3.json', ]):
    """
        Vi har brug for nogle felter at partitionere på, så jeg trækker år, måned og dag od af feltet 'Minutes5UTC'
        Input: en liste af filnavne, med path
        Output: en liste af processerede filenavne (Bør være de samme ...)
    """
    import json
    import datetime
    L = []
    for filename in json_falepathname_list:
        with open(filename, 'r') as f:
            j = json.load(f)
        records = []
        for record in j:
            timestamp = datetime.datetime.fromisoformat(record['Minutes5UTC'])
            record['year'] = timestamp.year
            record['month'] = timestamp.month
            record['day'] = timestamp.day
            # print(record)
            records.append(record)
        with open(filename, 'w+') as f:
            json.dump(records, f)
        # print(filename)
        L.append(filename)
    return L

def convert2partitioned_parquet(
        json_falepathname_list = ['./datafiles/datafile1.json', './datafiles/datafile2.json', './datafiles/datafile3.json', ],
        path = './'):
    """
        Nu eksporter jeg til parquet!
    """
    import json2parquet
    import json
    for filename in json_falepathname_list:
        # ds =json2parquet.load_json(filename)
        with open(filename, 'r') as f:
            j = json.load(f)
        ds = json2parquet.ingest_data(j)
        json2parquet.write_parquet_dataset(ds, path, partition_cols=['year','month','day', 'PriceArea'])
    return json_falepathname_list
    
def main():
    path = r'.\app\dags\EnergiDataService\data'

    print("collecting json filenames")
    js = collect_jsons(path + r'\back')
    print(len(js))

    print("merging jsons", end='')
    r =  merge_jsons(path + r'\back_collected', js)
    print(len(r))

    print("adding year, month and day")
    r = add_year_month_day(r)
    print(len(r))
    
    print("converting to parquet")
    # from glob import glob
    # r = glob(path + r'\back_collected\*.json')
    r = convert2partitioned_parquet(r, path=path + r'\parquet')
    # print(js)
    print(len(r))
    # print(r)

if __name__  == '__main__':
    main()