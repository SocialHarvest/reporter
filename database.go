// Social Harvest is a social media analytics platform.
//     Copyright (C) 2014 Tom Maiaroto, Shift8Creative, LLC (http://www.socialharvest.io)
//
//     This program is free software: you can redistribute it and/or modify
//     it under the terms of the GNU General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//
//     This program is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY; without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU General Public License for more details.
//
//     You should have received a copy of the GNU General Public License
//     along with this program.  If not, see <http://www.gnu.org/licenses/>.

// This file contains the database init and functions for querying it.
// Both Postgres and InfluxDB are supported.

package main

import (
	"github.com/SocialHarvest/harvester/lib/config"
	influxdb "github.com/influxdb/influxdb/client"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
)

type SocialHarvestDB struct {
	Postgres *sqlx.DB
	InfluxDB *influxdb.Client
	Series   []string
	Schema   struct {
		Compact bool `json:"compact"`
	}
}

var db = SocialHarvestDB{}

// Initializes the database and returns the client, setting it to `database.Postgres` in the current package scope
func newDatabase(c config.SocialHarvestConf) *SocialHarvestDB {
	// A database is not required to use Social Harvest
	if c.Database.Type == "" {
		return &db
	}
	var err error

	// Holds some options that will adjust the schema
	db.Schema = c.Schema

	// Now supporting Postgres OR InfluxDB
	// (for now...may add more in the future...the re-addition of InfluxDB is to satisfy performance curiosities, it may go away. Postgres will ALWAYS be supported.)
	// actually, if config.Database becomes an array, we can write to multiple databases...
	switch c.Database.Type {
	case "influxdb":
		cfg := &influxdb.ClientConfig{
			Host:       c.Database.Host + ":" + strconv.Itoa(c.Database.Port),
			Username:   c.Database.User,
			Password:   c.Database.Password,
			Database:   c.Database.Database,
			HttpClient: http.DefaultClient,
		}
		db.InfluxDB, err = influxdb.NewClient(cfg)
		if err != nil {
			log.Println(err)
			return &db
		}
	case "postgres", "postgresql":
		// Note that sqlx just wraps database/sql and `database.Postgres` gets a sqlx.DB which is essentially a wrapped sql.DB
		db.Postgres, err = sqlx.Connect("postgres", "host="+c.Database.Host+" port="+strconv.Itoa(c.Database.Port)+" sslmode=disable dbname="+c.Database.Database+" user="+c.Database.User+" password="+c.Database.Password)
		if err != nil {
			log.Println(err)
			return &db
		}
	}

	// Keep a list of series (tables/collections/series - whatever the database calls them, we're going with series because we're really dealing with time with just about all our data)
	// These do relate to structures in lib/config/series.go
	db.Series = []string{"messages", "shared_links", "mentions", "hashtags", "contributor_growth"}

	return &db
}

func Top() {

}
