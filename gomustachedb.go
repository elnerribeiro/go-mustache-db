package gomustachedb

import (
	sqlpack "database/sql"
	"errors"
	"io/ioutil"
	"runtime"
	"sync"
	"time"

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
var mapQueries = make(map[string]string)
var mapTimes = make(map[string]int64)
var isPostgres = false
var logger *ozzolog.Logger

//Dados e um hashmap
type Dados map[string]interface{}

func init() {
	runtime.SetFinalizer(logger, func() {
		if logger != nil {
			logger.Close()
		}
	})
}

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
		logError("[GetTransaction] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	tx, err := database.Begin()
	if err != nil {
		logError("[GetTransaction] Error starting transaction: %s", err)
		return nil, err
	}
	var trans Transacao
	trans.tx = tx
	return &trans, nil
}

// Delete remove uma linha na tabela
func Delete(transacao *Transacao, table string, filters Dados) (sqlpack.Result, error) {
	if database == nil {
		logError("[Delete] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	if transacao == nil || transacao.tx == nil {
		logError("[Delete] Transaction not found.")
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Delete(table, dbx.HashExp(filters))
	return q.Execute()
}

// Update altera uma linha na tabela
func Update(transacao *Transacao, table string, params Dados, filters Dados) (sqlpack.Result, error) {
	if database == nil {
		logError("[Update] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	if transacao == nil || transacao.tx == nil {
		logError("[Update] Transaction not found.")
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Update(table, dbx.Params(params), dbx.HashExp(filters))
	return q.Execute()
}

// Insert insere uma linha na tabela
func Insert(transacao *Transacao, table string, params Dados) (sqlpack.Result, error) {
	if database == nil {
		logError("[Insert] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	if transacao == nil || transacao.tx == nil {
		logError("[Insert] Transaction not found.")
		return nil, errors.New("not inside transaction")
	}
	q := transacao.tx.Insert(table, dbx.Params(params))
	return q.Execute()
}

// ExecuteSQL executa um SQL e traz o retorno
func ExecuteSQL(transacao *Transacao, query string, params Dados) (sqlpack.Result, error) {
	if database == nil {
		logError("[ExecuteSQL] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			logError("[ExecuteSQL] Error on mustache: %s", errM)
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
	q.Prepare()
	defer q.Close()
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	result, err2 := q.Execute()
	if err2 != nil {
		logError("[ExecuteSQL] Error executing SQL: %s", err2)
		return nil, err2
	}
	return result, nil
}

// InsertReturningPostgres executa um insert e traz o id inserido
func InsertReturningPostgres(transacao *Transacao, table string, params Dados, pk string, returnType interface{}) (interface{}, error) {
	if database == nil {
		logError("[InsertReturningPostgres] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	if !isPostgres {
		logError("[InsertReturningPostgres] Only works on Postgres.")
		return nil, errors.New("not in Postgres")
	}
	if transacao == nil || transacao.tx == nil {
		logError("[InsertReturningPostgres] Transaction not found.")
		return nil, errors.New("not inside transaction")
	}
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()
	var q *dbx.Query
	q = transacao.tx.Insert(table, dbx.Params(params))
	q.Execute()
	q = transacao.tx.NewQuery("select max(" + pk + ") as " + pk + " from " + table)
	err2 := q.One(returnType)
	if err2 != nil {
		logError("[InsertReturningPostgres] Error getting last inserted PK: %s", err2)
		return nil, err2
	}
	return returnType, nil
}

// SelectAll executa um select no banco e retorna todos os resultados
func SelectAll(transacao *Transacao, query string, returnType interface{}, params Dados) (interface{}, error) {
	if database == nil {
		logError("[SelectAll] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		logError("[SelectAll] Error reading query - %s: %s", query, err)
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			logError("[SelectAll] Error on mustache: %s", errM)
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
	q.Prepare()
	defer q.Close()
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	err2 := q.All(returnType)
	if err2 != nil {
		logError("[SelectAll] Error retrieving all rows: %s", err2)
		return nil, err2
	}
	return returnType, nil
}

// SelectOne executa um select no banco e retorna o primeiro resultado
func SelectOne(transacao *Transacao, query string, returnType interface{}, params Dados) (interface{}, error) {
	if database == nil {
		logError("[SelectOne] Database not initialized.")
		return nil, errors.New("database not initialized")
	}
	sql, err := sql(query)
	if err != nil {
		logError("[SelectOne] Error reading query - %s: %s", query, err)
		return nil, err
	}
	if params != nil {
		sqlIn, errM := mustache.Render(*sql, params)
		if errM != nil {
			logError("[SelectOne] Error on mustache: %s", errM)
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
	q.Prepare()
	defer q.Close()
	if params != nil {
		q.Bind(dbx.Params(params))
	}
	err2 := q.One(returnType)
	if err2 != nil {
		logError("[SelectOne] Error retrieving one row: %s", err2)
		return nil, err2
	}
	return returnType, nil
}

func sql(filename string) (*string, error) {
	if mapQueries[filename] != "" {
		retStr := mapQueries[filename]
		mapTimes[filename] = time.Now().Unix()
		return &retStr, nil
	}
	logInfo("Reading file: ./consultas/" + filename + ".sql")
	ret, err := ioutil.ReadFile("./consultas/" + filename + ".sql")
	if err != nil {
		logError("[sql] Error while reading file: %s", err)
		return nil, err
	}
	retStr := string(ret)
	insertIntoMap(filename, retStr)
	return &retStr, nil
}

func logInfo(info string) {
	if logger != nil {
		logger.Info(info)
	}
}

func logError(format string, a ...interface{}) {
	if logger != nil {
		logger.Error(format, a...)
	}
}

func insertIntoMap(filename, query string) {
	if len(mapQueries) >= 100 {
		var minTime = time.Now().Unix()
		var minKey = ""
		for k, v := range mapTimes {
			if v <= minTime {
				minKey = k
				minTime = v
			}
		}
		delete(mapQueries, minKey)
		delete(mapTimes, minKey)
	}
	mapQueries[filename] = query
	mapTimes[filename] = time.Now().Unix()
}

//InitDb inicializa a conexao com o DB
func InitDb(loggerParam *ozzolog.Logger, params ...string) error {
	if database != nil {
		logError("[InitDb] Database already initialized.")
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
	p := properties.MustLoadFile(*fileName, properties.UTF8)
	dialeto := p.GetString("dialeto", "")
	if dialeto != "" {
		db = &dialeto
	}
	isPostgres = (dialeto == "postgres")
	dbURL := p.GetString(*propertyURL, "")
	banco, err := dbx.Open(*db, dbURL)
	if err != nil {
		logError("[InitDb] Error connecting to database: %s", err)
		return err
	}
	printSQL = p.GetBool("printSql", false)
	database = banco
	logger = loggerParam
	if printSQL && logger != nil {
		database.LogFunc = logger.Info
		logger.Info("[InitDb] Connected to: " + dbURL)
	}
	return nil
}
