"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os
import pandas as pd  

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    carpeta="files/input"
    base=cargar_datos(carpeta)
    base_unida=unir_bases(base)
    base_limpia=limpiar_datos(base_unida)
    client,campaign,economics=separar_bases(base_limpia)
    guardar_datos(client,"client.csv")
    guardar_datos(campaign,"campaign.csv")
    guardar_datos(economics,"economics.csv")

    
def cargar_datos(carpeta):
    subcarpetas=os.listdir(carpeta)
    archivos = [f for f in subcarpetas if f.endswith('.zip') or f.endswith('.csv')]
    Bases=[]
    columnas_referencias=None

    for i in archivos:
        base_Datos=pd.read_csv(f"{carpeta}/{i}")
        if columnas_referencias is None:
            columnas_referencias=base_Datos.columns.tolist()
            base_Datos["Origin"]=i
            Bases.append(base_Datos)
        elif base_Datos.columns.tolist()==columnas_referencias:
            base_Datos["Origin"]=i
            Bases.append(base_Datos)
        else:
            raise ValueError("Las columnas de las bases de datos no coinciden")
    return Bases

def unir_bases(Bases):
    if not Bases:
        raise ValueError("La lista de bases de datos está vacía")
    base_final=pd.concat(Bases,ignore_index=True)

    base_final.drop(columns=["Unnamed: 0"],inplace=True,errors="ignore")
    
    return base_final

def dia_fecha(x):
    if int(x)<10:
        return f"0{x}"
    else:
        return str(x)

def limpiar_datos(base_Datos):
    base_Datos1=base_Datos.copy()
    base_Datos1["job"] = (
        base_Datos1["job"]
        .str.strip()
        .str.replace("-", "_", regex=False)
        .str.replace(".", "", regex=False)
    )

    base_Datos1["education"] = (
        base_Datos1["education"]
        .str.strip()
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA) 
    )
    col=["credit_default","mortgage","campaign_outcome"]
    for i in col:
        base_Datos1[i]=base_Datos1[i].apply(lambda x:1 if x=="yes" else 0)

    base_Datos1["previous_outcome"]=base_Datos1["previous_outcome"].apply(lambda x:1 if x=="success" else 0)

    base_Datos1["month"]=base_Datos1["month"].replace({
        "jan":"01",
        "feb":"02",
        "mar":"03",
        "apr":"04",
        "may":"05",
        "jun":"06",
        "jul":"07",
        "aug":"08",
        "sep":"09",
        "oct":"10",
        "nov":"11",
        "dec":"12"
    })

    base_Datos1["day"]=base_Datos1["day"].apply(dia_fecha)

    Fecha="2022"+"-"+base_Datos1["month"]+"-"+base_Datos1["day"]

    base_Datos1["last_contact_date"]=pd.to_datetime(Fecha,errors="coerce")

    return base_Datos1

def separar_bases(x):
    client=x[["client_id","age","job","marital","education","credit_default","mortgage"]].copy()

    campaign=x[["client_id","number_contacts","contact_duration","previous_campaign_contacts","previous_outcome","campaign_outcome","last_contact_date"]].copy()

    economics=x[["client_id","cons_price_idx","euribor_three_months"]].copy()

    return client,campaign,economics

def guardar_datos(df, nombre_archivo):
    carpeta_salida="files/output"
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)
    ruta_completa=os.path.join(carpeta_salida,nombre_archivo)
    df.to_csv(ruta_completa,index=False)

if __name__ == "__main__":
    clean_campaign_data()
