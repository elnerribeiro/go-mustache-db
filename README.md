# gosbxdb
Biblioteca para acesso a dados (Postgres e MySQL)

import "github.com/elnerribeiro/gosbxdb"

Arquivos necessários:

. Arquivo application.properties:

	printSql=false
	
	db.url=postgres://USERNAME:PASSWORD@HOST:PORTA/BANCO (Postgres)
	
ou

	db.url=username:password@host:porta/banco?param=value (MySQL)
	
. Pasta consultas/
	. Arquivos terminados em .sql

