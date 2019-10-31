package gosbxdb

import (
	sqlpack "database/sql"
	"errors"
	"io/ioutil"

	mustache "github.com/cbroglie/mustache"
	dbx "github.com/go-ozzo/ozzo-dbx"
	ozzolog "github.com/go-ozzo/ozzo-log"

	//driver postgres
	_ "github.com/lib/pq"
	//driver mysql
	_ "github.com/go-sql-driver/mysql"
	"github.com/magiconair/properties"
)

var database *dbx.DB = nil
var printSQL bool

//Dados e um hashmap
type Dados map[string]interface{}

//Transacao e uma transacao do banco de dados
type Transacao struct {
	tx *dbx.Tx
}

//Commit commita a transacao
func Commit(tx *Transacao) {
	tx.tx.Commit()
}

//Rollback rollback na transacao
func Rollback(tx *Transacao) {
	tx.tx.Rollback()
}

//GetTransaction retorna uma transacao
func GetTransaction() (*Transacao, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	tx, err := database.Begin()
	if err != nil {
		return nil, err
	}
	var trans Transacao
	trans.tx = tx
	return &trans, nil
}

// Delete remove uma linha na tabela
func Delete(transacao *Transacao, table string, filters Dados) (sqlpack.Result, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	if transacao == nil && transacao.tx == nil {
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Delete(table, dbx.HashExp(filters))
	return q.Execute()
}

// Update altera uma linha na tabela
func Update(transacao *Transacao, table string, params Dados, filters Dados) (sqlpack.Result, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	if transacao == nil && transacao.tx == nil {
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Update(table, dbx.Params(params), dbx.HashExp(filters))
	return q.Execute()
}

// Insert insere uma linha na tabela
func Insert(transacao *Transacao, table string, params Dados) (sqlpack.Result, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	if transacao == nil && transacao.tx == nil {
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Insert(table, dbx.Params(params))
	return q.Execute()
}

// ExecuteSQL executa um SQL e traz o retorno
func ExecuteSQL(transacao *Transacao, query string, params Dados) (sqlpack.Result, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			return nil, errM
		}
		sql = &sqlIn
	}
	var q *dbx.Query
	if transacao != nil && transacao.tx != nil {
		q = transacao.tx.NewQuery(*sql)
	} else {
		q = database.NewQuery(*sql)
	}
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	result, err2 := q.Execute()
	if err2 != nil {
		return nil, err2
	}
	return result, nil
}

// SelectAll executa um select no banco e retorna todos os resultados
func SelectAll(transacao *Transacao, query string, returnType interface{}, params Dados) (interface{}, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			return nil, errM
		}
		sql = &sqlIn
	}
	var q *dbx.Query
	if transacao != nil && transacao.tx != nil {
		q = transacao.tx.NewQuery(*sql)
	} else {
		q = database.NewQuery(*sql)
	}
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	err2 := q.All(returnType)
	if err2 != nil {
		return nil, err2
	}
	return returnType, nil
}

// SelectOne executa um select no banco e retorna o primeiro resultado
func SelectOne(transacao *Transacao, query string, returnType interface{}, params Dados) (interface{}, error) {
	if database == nil {
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			return nil, errM
		}
		sql = &sqlIn
	}
	var q *dbx.Query
	if transacao != nil && transacao.tx != nil {
		q = transacao.tx.NewQuery(*sql)
	} else {
		q = database.NewQuery(*sql)
	}
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	err2 := q.One(returnType)
	if err2 != nil {
		return nil, err2
	}
	return returnType, nil
}

func sql(filename string) (*string, error) {
	ret, err := ioutil.ReadFile("./consultas/" + filename + ".sql")
	if err != nil {
		return nil, err
	}
	retStr := string(ret)
	return &retStr, nil
}

func initDb(params ...string) error {
	if database != nil {
		return nil
	}
	fileNameStr := "application.properties"
	dbStr := "postgres"
	propertyURLStr := "db.url"
	fileName := &fileNameStr
	db := &dbStr
	propertyURL := &propertyURLStr
	if len(params) == 1 {
		fileName = &params[0]
	}
	if len(params) == 2 {
		fileName = &params[0]
		db = &params[1]
	}
	if len(params) == 3 {
		fileName = &params[0]
		db = &params[1]
		propertyURL = &params[2]
	}
	p := properties.MustLoadFile(*fileName, properties.UTF8)
	banco, err := dbx.Open(*db, p.GetString(*propertyURL, ""))
	printSQL = p.GetBool("printSql", false)
	if err != nil {
		return err
	}
	database = banco
	if printSQL {
		logger := ozzolog.NewLogger()
		logger.Targets = []ozzolog.Target{ozzolog.NewConsoleTarget()}
		logger.Open()
		database.LogFunc = logger.Info
	}
	return nil
}