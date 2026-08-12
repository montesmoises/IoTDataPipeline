"""
Lógica de negocio pura del recolector.

Regla del paquete: aquí NO se importa pyodbc, pymcprotocol, customtkinter ni
nada que hable con el mundo exterior. Todo es determinista y se prueba sin PLC
y sin base de datos, en milisegundos.

Si una función necesita la hora actual, la configuración de turnos o cualquier
otro dato del entorno, se le pasa como argumento. Quien lo obtiene del mundo
real es Prensas.py.
"""
