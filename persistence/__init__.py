"""
Acceso a datos: conexiones, SQL y archivo de estado.

Todo el SQL contra SQL Server vive en repositorio.py. Si cambia la forma de
registrar la producción, ese es el único archivo que hay que tocar.

Las funciones reciben el `cursor` de quien las llama: la transacción la maneja
el llamador, que es quien sabe dónde empieza y termina una unidad de trabajo.
"""
