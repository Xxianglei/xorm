// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"context"
	"database/sql"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"sync"
	"time"
)

// DriverType database driver constant int.
type DriverType int

// Enum the Database driver
const (
	_       DriverType = iota // int enum type
	DRMySQL                   // mysql
)

// database driver string.
type driver string

// get type constant int of current driver..
func (d driver) Type() DriverType {
	a, _ := dataBaseCache.get(string(d))
	return a.Driver
}

// get name of current driver
func (d driver) Name() string {
	return string(d)
}

// check driver iis implemented Driver interface or not.
var _ Driver = new(driver)

var (
	dataBaseCache = &_dbCache{cache: make(map[string]*alias)}
	drivers       = map[string]DriverType{
		"mysql": DRMySQL,
	}
	dbBasers = map[DriverType]dbBaser{
		DRMySQL: newdbBaseMysql(),
	}
)

// database alias cacher.
type _dbCache struct {
	mux   sync.RWMutex
	cache map[string]*alias
}

// add database alias with original name.
func (ac *_dbCache) add(name string, al *alias) (added bool) {
	ac.mux.Lock()
	defer ac.mux.Unlock()
	if _, ok := ac.cache[name]; !ok {
		ac.cache[name] = al
		added = true
	}
	return
}

// get database alias if cached.
func (ac *_dbCache) get(name string) (al *alias, ok bool) {
	ac.mux.RLock()
	defer ac.mux.RUnlock()
	al, ok = ac.cache[name]
	return
}

// get default alias.
func (ac *_dbCache) getDefault() (al *alias) {
	al, _ = ac.get("default")
	return
}

type DB struct {
	*sync.RWMutex
	DB             *sql.DB
	stmtDecorators *lru.Cache
}

func (d *DB) Begin() (*sql.Tx, error) {
	return d.DB.Begin()
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.DB.BeginTx(ctx, opts)
}

//su must call release to release *sql.Stmt after using
func (d *DB) getStmtDecorator(query string) (*stmtDecorator, error) {
	d.RLock()
	c, ok := d.stmtDecorators.Get(query)
	if ok {
		c.(*stmtDecorator).acquire()
		d.RUnlock()
		return c.(*stmtDecorator), nil
	}
	d.RUnlock()

	d.Lock()
	c, ok = d.stmtDecorators.Get(query)
	if ok {
		c.(*stmtDecorator).acquire()
		d.Unlock()
		return c.(*stmtDecorator), nil
	}

	stmt, err := d.Prepare(query)
	if err != nil {
		d.Unlock()
		return nil, err
	}
	sd := newStmtDecorator(stmt)
	sd.acquire()
	d.stmtDecorators.Add(query, sd)
	d.Unlock()

	return sd, nil
}

func (d *DB) Prepare(query string) (*sql.Stmt, error) {
	return d.DB.Prepare(query)
}

func (d *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.DB.PrepareContext(ctx, query)
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.Exec(args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.ExecContext(ctx, args...)
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.Query(args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		return nil, err
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryContext(ctx, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		panic(err)
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryRow(args...)

}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	sd, err := d.getStmtDecorator(query)
	if err != nil {
		panic(err)
	}
	stmt := sd.getStmt()
	defer sd.release()
	return stmt.QueryRowContext(ctx, args)
}

type alias struct {
	Name         string
	Driver       DriverType
	DriverName   string
	DataSource   string
	MaxIdleConns int
	MaxOpenConns int
	DB           *DB
	DbBaser      dbBaser
	TZ           *time.Location
	Engine       string
}

func addAliasWthDB(aliasName, driverName string, db *sql.DB) (*alias, error) {
	al := new(alias)
	al.Name = aliasName
	al.DriverName = driverName
	al.Engine = "InnoDB"
	al.DB = &DB{
		RWMutex:        new(sync.RWMutex),
		DB:             db,
		stmtDecorators: newStmtDecoratorLruWithEvict(),
	}

	if dr, ok := drivers[driverName]; ok {
		al.DbBaser = dbBasers[dr]
		al.Driver = dr
	} else {
		return nil, fmt.Errorf("driver name `%s` have not registered", driverName)
	}

	err := db.Ping()
	if err != nil {
		return nil, fmt.Errorf("register db Ping `%s`, %s", aliasName, err.Error())
	}

	if !dataBaseCache.add(aliasName, al) {
		return nil, fmt.Errorf("DataBase alias name `%s` already registered, cannot reuse", aliasName)
	}

	return al, nil
}

// RegisterDataBase Setting the database connect params. Use the database driver self dataSource args.
func RegisterDataBase(aliasName, driverName string, db *sql.DB) error {
	var (
		err error
	)
	_, err = addAliasWthDB(aliasName, driverName, db)
	if err != nil {
		goto end
	}

end:
	if err != nil {
		if db != nil {
			db.Close()
		}
		DebugLog.Println(err.Error())
	}

	return err
}

// GetDB Get *sql.DB from registered database by db alias name.
// Use "default" as alias name if you not set.
func GetDB(aliasNames ...string) (*sql.DB, error) {
	var name string
	if len(aliasNames) > 0 {
		name = aliasNames[0]
	} else {
		name = "default"
	}
	al, ok := dataBaseCache.get(name)
	if ok {
		return al.DB.DB, nil
	}
	return nil, fmt.Errorf("DataBase of alias name `%s` not found", name)
}

type stmtDecorator struct {
	wg      sync.WaitGroup
	lastUse int64
	stmt    *sql.Stmt
}

func (s *stmtDecorator) getStmt() *sql.Stmt {
	return s.stmt
}

func (s *stmtDecorator) acquire() {
	s.wg.Add(1)
	s.lastUse = time.Now().Unix()
}

func (s *stmtDecorator) release() {
	s.wg.Done()
}

//garbage recycle for stmt
func (s *stmtDecorator) destroy() {
	go func() {
		s.wg.Wait()
		_ = s.stmt.Close()
	}()
}

func newStmtDecorator(sqlStmt *sql.Stmt) *stmtDecorator {
	return &stmtDecorator{
		stmt:    sqlStmt,
		lastUse: time.Now().Unix(),
	}
}

func newStmtDecoratorLruWithEvict() *lru.Cache {
	cache, _ := lru.NewWithEvict(1000, func(key interface{}, value interface{}) {
		value.(*stmtDecorator).destroy()
	})
	return cache
}
