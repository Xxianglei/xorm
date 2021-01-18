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

// +build go1.8

// Package orm provide ORM for MySQL/PostgreSQL/sqlite
// Simple Usage
//
//	package main
//
//	import (
//		"fmt"
//		"github.com/astaxie/beego/orm"
//		_ "github.com/go-sql-driver/mysql" // import your used driver
//	)
//
//	// Model Struct
//	type User struct {
//		Id   int    `orm:"auto"`
//		Name string `orm:"size(100)"`
//	}
//
//	func init() {
//		orm.RegisterDataBase("default", "mysql", "root:root@/my_db?charset=utf8", 30)
//	}
//
//	func main() {
//		o := orm.NewOrm()
//		user := User{Name: "slene"}
//		// insert
//		id, err := o.Insert(&user)
//		// update
//		user.Name = "astaxie"
//		num, err := o.Update(&user)
//		// read one
//		u := User{Id: user.Id}
//		err = o.Read(&u)
//		// delete
//		num, err = o.Delete(&u)
//	}
//
// more docs: http://beego.me/docs/mvc/model/overview.md
package orm

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
)

// DebugQueries define the debug
const (
	DebugQueries = iota
)

// Define common vars
var (
	Debug            = false
	DebugLog         = NewLog(os.Stdout)
	DefaultRowsLimit = -1
	DefaultRelsDepth = 2
	DefaultTimeLoc   = time.Local
	ErrTxHasBegan    = errors.New("<Ormer.Begin> transaction already begin")
	ErrTxDone        = errors.New("<Ormer.Commit/Rollback> transaction not begin")
	ErrMultiRows     = errors.New("<QuerySeter> return multi rows")
	ErrNoRows        = errors.New("<QuerySeter> no row found")
	ErrStmtClosed    = errors.New("<QuerySeter> stmt already closed")
	ErrArgs          = errors.New("<Ormer> args error may be empty")
	ErrNotImplement  = errors.New("have not implement")
)

// Params stores the Params
type Params map[string]interface{}

// ParamsList stores paramslist
type ParamsList []interface{}

type orm struct {
	alias *alias
	db    dbQuerier
	isTx  bool
}

// get model info and model reflect value
func (o *orm) getMiInd(md interface{}, needPtr bool) (mi *modelInfo, ind reflect.Value) {
	val := reflect.ValueOf(md)
	ind = reflect.Indirect(val)
	typ := ind.Type()
	if needPtr && val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("<Ormer> cannot use non-ptr model struct `%s`", getFullName(typ)))
	}
	name := getFullName(typ)
	if mi, ok := modelCache.getByFullName(name); ok {
		return mi, ind
	}
	panic(fmt.Errorf("<Ormer> table: `%s` not found, make sure it was registered with `RegisterModel()`", name))
}

// get field info from model info by given field name
func (o *orm) getFieldInfo(mi *modelInfo, name string) *fieldInfo {
	fi, ok := mi.fields.GetByAny(name)
	if !ok {
		panic(fmt.Errorf("<Ormer> cannot find field `%s` for model `%s`", name, mi.fullName))
	}
	return fi
}

// return current using database Driver
func (o *orm) Driver() Driver {
	return driver(o.alias.Name)
}

// return sql.DBStats for current database
func (o *orm) DBStats() *sql.DBStats {
	if o.alias != nil && o.alias.DB != nil {
		stats := o.alias.DB.DB.Stats()
		return &stats
	}
	return nil
}
