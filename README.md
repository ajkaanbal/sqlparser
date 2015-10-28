# SQL Parser to OrientDB graph

## Dependencias
  
  `pip install -r requeriments.txt`

## Usage: python sql2graph.py [OPTIONS]

Options:
  --host TEXT      OrientDB server
  --port INTEGER   Puerto para conectarse a OrientDB
  --user TEXT      Nombre de usuario de acceso a OrientDB  [required]
  --password TEXT
  --database TEXT  Nombre de base de datos en orientdb
  --path TEXT      Carpeta con archivos sql
  --help           Show this message and exit.


## TEST

  py.test
