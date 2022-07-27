from csv import reader

c=59
i=1
PS=""
string="["
with open('schema.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    header = next(csv_reader)
    if header != None:
        for row in csv_reader:
            if i < c:
                PS=str(PS)+f"bigquery.SchemaField(\"{row[0]}\", bigquery.enums.SqlTypeNames.{row[1]}),\n"
                string=str(string)+"""    {
            "name": \""""+row[0]+"""\",
            "type": \""""+row[1]+"""\",
            "mode": \""""+row[2]+"""\"
            },\n"""
                i+=1
            else:
                PS=str(PS)+f"bigquery.SchemaField(\"{row[0]}\", bigquery.enums.SqlTypeNames.{row[1]})\n"
                string=str(string)+"""    {
            "name": \""""+row[0]+"""\",
            "type": \""""+row[1]+"""\",
            "mode": \""""+row[2]+"""\"
            }\n]"""

print(string)
#print(PS)