from pyparsing import Path
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error # poner este como metrica
import matplotlib.pyplot as plt
import seaborn as sns 
from os import listdir
from os.path import isfile, join
from datetime import date, time, datetime
from pyparsing import Path
from fuzzywuzzy import process
from fuzzywuzzy import fuzz 
ds_path = Path(".\Datasets")

cadena_conexion = 'mysql+pymysql://root:@localhost:3306/ProyectIndiv'
conexion = create_engine(cadena_conexion)

nombres=[]
proporcion=[]
def cambios ( mal, bien):
    for i in mal:
        x=process.extractOne(i,bien)
        nombres.append(x[0])
        proporcion.append(x[1])
    return(nombres, proporcion)

map_list = []

def rename ( mal, bien):
    for name in mal:
        best_ratio = None
        for idx, surname in enumerate(bien):
            if best_ratio == None:
                best_ratio = fuzz.ratio(name, surname)
                best_idx = 0
            else:
                ratio = fuzz.ratio(name, surname)
                if  ratio > best_ratio:
                    best_ratio = ratio
                    best_idx = idx
        map_list.append(bien[best_idx]) # obtain surname

#dico = dico[["Name", "Surname", "Age", "Studies"]] # reorder columns

Arc_Localidades=[]
for file in ds_path.glob('localidades*'):
    df = pd.read_csv(file,decimal =",") 
    Arc_Localidades.append(df)
Localidades=pd.concat(Arc_Localidades)
Localidades = Localidades.rename(columns={'centroide_lon':'Longitud','centroide_lat':'Latitud', 'id':'Id_Localidades', 'categoria':'Categoria', 'departamento_id':'Departamento_Id', 'departamento_nombre':'Departamento_Nombre',
'fuente':'Fuente','localidad_censal_id':'Localidad_Censal_Id','localidad_censal_nombre':'Localidad_Censal_Nombre','municipio_id':'Municipio_Id','municipio_nombre':'Municipio_Nombre','nombre':'Nombre','provincia_id':'Provincia_Id','provincia_nombre':'Provincia_Nombre' })
Localidades['Departamento_Id'] = Localidades['Departamento_Id'].replace(np.NaN,"1")
Localidades['Municipio_Id'] = Localidades['Municipio_Id'].replace(np.NaN,"1")
Localidades = Localidades.astype({'Departamento_Id':'double','Municipio_Id':'double'})
Localidades = Localidades.astype({'Departamento_Id':'int64','Municipio_Id':'int64'})
Localidades['Longitud']= pd.to_numeric(Localidades['Longitud'],errors='coerce')
Localidades['Longitud'] = Localidades['Longitud'].astype(float)
Localidades['Latitud']= pd.to_numeric(Localidades['Latitud'],errors='coerce')
Localidades['Latitud'] = Localidades['Latitud'].astype(float)
Localidades_final = Localidades.copy()
Provincia = Localidades.loc[:,['Provincia_Id','Provincia_Nombre']]
Provincia = Provincia.drop_duplicates()
Provincia = Provincia.set_index('Provincia_Id')
Departamento = Localidades.loc[:,['Departamento_Id','Departamento_Nombre','Provincia_Id']]
Departamento = Departamento.drop_duplicates()
Departamento = Departamento.set_index('Departamento_Id')
Municipio = Localidades.loc[:,['Municipio_Id','Municipio_Nombre','Departamento_Id']]
Municipio = Municipio.drop_duplicates()
Municipio = Municipio.set_index('Municipio_Id')
Localidad = Localidades.loc[:,['Localidad_Censal_Id','Localidad_Censal_Nombre','Municipio_Id']]
Localidad = Localidad.drop_duplicates()
Localidad = Localidad.set_index('Localidad_Censal_Id')
Localidades_final.drop(['Provincia_Nombre','Departamento_Nombre','Municipio_Nombre','Localidad_Censal_Nombre'], axis = 1, inplace = True)
Localidades_final = Localidades_final.set_index('Id_Localidades')
Localidades = Localidades.set_index('Id_Localidades')
Provincia.to_sql(name='provincia', con=conexion, if_exists='append')
Departamento.to_sql(name='departamento', con=conexion, if_exists='append')
Municipio.to_sql(name='municipio', con=conexion, if_exists='append')
Localidad.to_sql(name='localidad', con=conexion, if_exists='append')
Localidades_final.to_sql(name='localidades_final', con=conexion, if_exists='append')
#Localidades.to_sql(name='localidades_final', con=conexion, if_exists='append')

Arc_TipoGastos=[]
for file in ds_path.glob('TiposDeGasto*'):
    df = pd.read_csv(file)
    Arc_TipoGastos.append(df)
Tipo_Gastos=pd.concat(Arc_TipoGastos)
Tipo_Gastos = Tipo_Gastos.set_index('IdTipoGasto')
Tipo_Gastos.to_sql(name='tipo_Gastos', con=conexion, if_exists='append')


Arc_Clientes=[]
for file in ds_path.glob('Cliente*'):
    df = pd.read_csv(file,delimiter = ';',decimal =",", encoding="UTF-8") 
    Arc_Clientes.append(df)
Clientes=pd.concat(Arc_Clientes)
Clientes.drop('col10', axis = 1, inplace = True)
Clientes['Y'] = Clientes['Y'].str.replace(',','.')
Clientes['X'] = Clientes['X'].str.replace(',','.')
Clientes  = Clientes.rename(columns={'X':'Longitud','Y':'Latitud','ID':'Id_Cliente'})
Clientes['Longitud']= pd.to_numeric(Clientes['Longitud'],errors='coerce')
Clientes['Longitud'] = Clientes['Longitud'].astype(float)
Clientes['Latitud']= pd.to_numeric(Clientes['Latitud'],errors='coerce')
Clientes['Latitud'] = Clientes['Latitud'].astype(float)
Clientes['Localidad'] = Clientes['Localidad'].str.capitalize() 
Clientes = Clientes.set_index('Id_Cliente')
Clientes.to_sql(name='Clientes', con=conexion, if_exists='append')
    
Arc_Sucursales=[]
for file in ds_path.glob('Sucursales*'):
    df = pd.read_csv(file,delimiter = ';')
    Arc_Sucursales.append(df)
Sucursales=pd.concat(Arc_Sucursales)
Sucursales['Latitud'] = Sucursales['Latitud'].str.replace(',','.')
Sucursales['Longitud'] = Sucursales['Longitud'].str.replace(',','.')
Sucursales = Sucursales.astype({'Longitud':'float','Latitud':'float'})
Sucursales  = Sucursales.rename(columns={'ID':'Id_Sucursal'})
#Sucursales['Sum'] = Sucursales['Longitud'] + Sucursales['Latitud']
Sucursales['Localidad']=Sucursales['Localidad'].replace(['Ciudad de Buenos Aires', 'CABA', 'Capital', 'Capital Federal', 'CapFed', 'Cap. Fed.', 'Cap.   Federal', 'Cdad de Buenos Aires'], 'Ciudad Autónoma de Buenos Aires')
Sucursales['Localidad']=Sucursales['Localidad'].replace(['Coroba', 'Cordoba'],'Córdoba')
Sucursales['Provincia']=Sucursales['Provincia'].replace(['Ciudad de Buenos Aires', 'CABA', 'C deBuenos Aires', 'Bs As', 'Bs.As. ', 'B. Aires', 'B.Aires', 'Provincia de Buenos Aires', 'Prov de Bs As.', 'Pcia Bs AS'], 'Buenos Aires')
Sucursales['Provincia']=Sucursales['Provincia'].replace(['Coroba', 'Cordoba'],'Córdoba')
Sucursales = Sucursales.set_index('Id_Sucursal')
Sucursales.to_sql(name='Sucursales', con=conexion, if_exists='append')

Arc_Gastos=[]
for file in ds_path.glob('Gasto*'):
    df = pd.read_csv(file,delimiter = ',')
    Arc_Gastos.append(df)
Gastos=pd.concat(Arc_Gastos)
Gastos['Fecha'] = pd.to_datetime(Gastos['Fecha'])
Gastos = Gastos.set_index('IdGasto')
Gastos.to_sql(name='Gastos', con=conexion, if_exists='append')

Arc_Proveedores=[]
for file in ds_path.glob('Proveedores*'):
    df = pd.read_csv(file,delimiter = ',',decimal =",", encoding="ansi") 
    Arc_Proveedores.append(df)
Proveedores=pd.concat(Arc_Proveedores)
Proveedores['State']=Proveedores['State'].replace('CABA', 'BUENOS AIRES')
Proveedores['City'] = Proveedores['City'].str.capitalize() 
Proveedores['State'] = Proveedores['State'].str.capitalize() 
Proveedores['Country'] = Proveedores['Country'].str.capitalize() 
Proveedores['departamen'] = Proveedores['departamen'].str.capitalize() 
Proveedores = Proveedores.set_index('IDProveedor')
Proveedores.to_sql(name='Proveedores', con=conexion, if_exists='append')

Arc_Compras=[]
for file in ds_path.glob('Compra*'):
    df = pd.read_csv(file,delimiter = ',',decimal =",", encoding="UTF-8") 
    Arc_Compras.append(df)
Compras=pd.concat(Arc_Compras)
Compras['Fecha'] = pd.to_datetime(Compras['Fecha'])
Compras = Compras.set_index('IdCompra')
Compras.to_sql(name='Compras', con=conexion, if_exists='append')
    
Arc_CanalVenta=[]
for file in ds_path.glob('Canal*'):
    df = pd.read_excel(file,decimal =",") 
    Arc_CanalVenta.append(df)
CanalVenta=pd.concat(Arc_CanalVenta)
CanalVenta  = CanalVenta.rename(columns={'CODIGO':'IdCanal'})
CanalVenta = CanalVenta.set_index('IdCanal')
CanalVenta.to_sql(name='CanalVenta', con=conexion, if_exists='append')

Arc_Ventas=[]
for file in ds_path.glob('Venta*'):
    df = pd.read_csv(file,delimiter = ',',decimal =",", encoding="UTF-8") 
    Arc_Ventas.append(df)
Ventas=pd.concat(Arc_Ventas)
Ventas['Fecha'] = pd.to_datetime(Ventas['Fecha'])
Ventas['Fecha_Entrega'] = pd.to_datetime(Ventas['Fecha_Entrega'])
Ventas = Ventas.set_index('IdVenta')
Ventas.to_sql(name='Ventas', con=conexion, if_exists='append')





#cnn = mysql.connector.connect( host = "localhost", user = "root", passwd ="",database = "ProyectIndiv")
#print (cnn)

#Provincia.to_sql(name='Provincia', con=cnn)