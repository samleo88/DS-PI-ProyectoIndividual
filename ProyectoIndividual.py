import csv

path =  './datasets/Proveedores.csv'

with open(path, 'r',  newline='') as infile,\
     open('Proveedores.csv', 'w',  encoding='utf8') as outfile:
     inputs = csv.reader(infile, quotechar='"')
     output = csv.writer(outfile, quotechar='"')
     output.writerows(row for row in inputs)

# with open(path, 'r', encoding='utf-16-le',  newline='') as infile,\
#     open('final.csv', 'w',  encoding='utf-8', newline='') as outfile:
#     inputs = csv.reader(infile, delimiter="\t", quotechar='"')
#     output = csv.writer(outfile, delimiter=",", quotechar='"')
#     next(inputs) # Descartar primera fila (header)
#     output.writerows(row for row in inputs)