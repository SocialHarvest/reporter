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
	"bytes"
	"github.com/SocialHarvest/harvester/lib/config"
	influxdb "github.com/influxdb/influxdb/client"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	//"github.com/mitchellh/mapstructure"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
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

// Checks access to the database
func (db *SocialHarvestDB) HasAccess() bool {
	var err error

	if db.Postgres != nil {
		var c int
		err = db.Postgres.Get(&c, "SELECT COUNT(*) FROM messages")
		if err == nil {
			return true
		} else {
			return false
		}
	}
	if db.InfluxDB != nil {

	}

	return false
}

// -------- GETTING STUFF BACK OUT ------------
// Note: We're a little stuck in the ORM and prepared statement department because our queries need to be pretty flexible.
// Table names are dynamic in some cases (rules out prepared statements) and we have special functions and "AS" keywords all over,
// so most ORMs are out because they are designed for basic CRUD. Upper.io wasn't the most robust ORM either, but it supported quite
// a few databases and worked well for the writes. The reading was always going to be a challenge. We luck out a little bit with using
// the CommonQueryParams struct because we know the Limit, for example, must be an int and therefore is sanitized already.
// Sanitizing data won't be so bad though because we're only allowing a limited amount of user input to begin with.

// Some common parameters to make passing them around a bit easier
type CommonQueryParams struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Territory string `json:"territory"`
	Network   string `json:"network,omitempty"`
	Field     string `json:"field,omitempty"`
	Limit     uint   `json:"limit,omitempty"`
	Series    string `json:"series,omitempty"`
	Skip      uint   `json:"skip,omitempty"`
}

type ResultCount struct {
	Count    int    `json:"count"`
	TimeFrom string `json:"timeFrom"`
	TimeTo   string `json:"timeTo"`
}

type ResultAggregateCount struct {
	Count int    `json:"count"`
	Value string `json:"value"`
}

type ResultAggregateAverage struct {
	Average int    `json:"average"`
	Value   string `json:"value"`
}

type ResultAggregateFields struct {
	Count    map[string][]ResultAggregateCount   `json:"counts,omitempty"`
	Average  map[string][]ResultAggregateAverage `json:"averages,omitempty"`
	TimeFrom string                              `json:"timeFrom"`
	TimeTo   string                              `json:"timeTo"`
	Total    int                                 `json:"total"`
}

type MessageConditions struct {
	Gender     string `json:"contributor_gender,omitempty"`
	Lang       string `json:"contributor_lang,omitempty"`
	Country    string `json:"contributor_country,omitempty"`
	IsQuestion int    `json:"is_question,omitempty"`
	Geohash    string `json:"contributor_geohash,omitempty"`
}

// Sanitizes common query params to prevent SQL injection and to ensure proper formatting, etc.
func SanitizeCommonQueryParams(params CommonQueryParams) CommonQueryParams {
	sanitizedParams := CommonQueryParams{}

	// Just double check it's positive
	if params.Limit > 0 {
		sanitizedParams.Limit = params.Limit
	}
	if params.Skip > 0 {
		sanitizedParams.Skip = params.Skip
	}

	// Prepared statements not so good when we let users dynamically chose the table to query (neither are any of the ORMs for Golang either unfortunately).
	// Only allow tables speicfied in the series slice to be used in a query.
	for _, v := range config.SeriesCollections {
		if params.Series == v {
			sanitizedParams.Series = params.Series
		}
	}

	// Territory names can included spaces and are alphanumeric
	pattern := `(?i)[A-z0-9\s]`
	r, _ := regexp.Compile(pattern)
	if r.MatchString(params.Territory) {
		sanitizedParams.Territory = params.Territory
	}

	// Field (column) names and Network names can contain letters, numbers, and underscores
	pattern = `(?i)[A-z0-9\_]`
	r, _ = regexp.Compile(pattern)
	if r.MatchString(params.Field) {
		sanitizedParams.Field = params.Field
	}
	r, _ = regexp.Compile(pattern)
	if r.MatchString(params.Network) {
		sanitizedParams.Network = params.Network
	}

	// to/from are dates and there's only certain characters necessary there too. Fore xample, something like 2014-08-08 12:00:00 is all we need.
	// TODO: Maybe timezone too? All dates should be UTC so there may really be no need.
	// Look for anything other than numbers, a single dash, colons, and spaces. Then also trim a dash at the end of the string in case. It's an invalid query really, but let it work still (for now).
	pattern = `\-{2,}|\"|\'|[A-z]|\#|\;|\*|\!|\\|\/|\(|\)|\|`
	r, _ = regexp.Compile(pattern)
	if !r.MatchString(params.To) {
		sanitizedParams.To = strings.Trim(params.To, "-")
	}
	if !r.MatchString(params.From) {
		sanitizedParams.From = strings.Trim(params.From, "-")
	}

	//log.Println(sanitizedParams)
	return sanitizedParams
}

// Groups fields values and returns a count of occurences
func (db *SocialHarvestDB) FieldCounts(queryParams CommonQueryParams, fields []string) ([]ResultAggregateFields, ResultCount) {
	var fieldCounts []ResultAggregateFields
	var total ResultCount
	sanitizedQueryParams := SanitizeCommonQueryParams(queryParams)

	if db.Postgres != nil {
		// The following query should work for pretty much any SQL database (at least any we're supporting)
		var err error

		// First get the overall total number of records
		var buffer bytes.Buffer
		buffer.WriteString("SELECT COUNT(*) AS count FROM ")
		buffer.WriteString(sanitizedQueryParams.Series)
		buffer.WriteString(" WHERE territory = '")
		buffer.WriteString(sanitizedQueryParams.Territory)
		buffer.WriteString("'")
		// optional date range (can have either or both)
		if sanitizedQueryParams.From != "" {
			buffer.WriteString(" AND time >= '")
			buffer.WriteString(sanitizedQueryParams.From)
			buffer.WriteString("'")
		}
		if sanitizedQueryParams.To != "" {
			buffer.WriteString(" AND time <= '")
			buffer.WriteString(sanitizedQueryParams.To)
			buffer.WriteString("'")
		}

		tQuery := buffer.String()
		buffer.Reset()
		err = db.Postgres.Get(&total, tQuery)
		if err != nil {
			log.Println(err)
		}

		for _, field := range fields {
			if len(field) > 0 {
				buffer.Reset()
				buffer.WriteString("SELECT COUNT(")
				buffer.WriteString(field)
				buffer.WriteString(") AS count,")
				buffer.WriteString(field)
				buffer.WriteString(" AS value")
				buffer.WriteString(" FROM ")
				buffer.WriteString(sanitizedQueryParams.Series)
				buffer.WriteString(" WHERE territory = '")
				buffer.WriteString(sanitizedQueryParams.Territory)
				buffer.WriteString("'")

				// optional date range (can have either or both)
				if sanitizedQueryParams.From != "" {
					buffer.WriteString(" AND time >= '")
					buffer.WriteString(sanitizedQueryParams.From)
					buffer.WriteString("'")
				}
				if sanitizedQueryParams.To != "" {
					buffer.WriteString(" AND time <= '")
					buffer.WriteString(sanitizedQueryParams.To)
					buffer.WriteString("'")
				}

				buffer.WriteString(" GROUP BY ")
				buffer.WriteString(field)

				buffer.WriteString(" ORDER BY count DESC")

				// optional limit (in this case I don't know why one would use it - a date range would be a better limiter)
				if sanitizedQueryParams.Limit > 0 {
					buffer.WriteString(" LIMIT ")
					buffer.WriteString(strconv.FormatInt(int64(sanitizedQueryParams.Limit), 10))
				}

				query := buffer.String()
				buffer.Reset()

				var valueCounts []ResultAggregateCount
				err = db.Postgres.Select(&valueCounts, query)

				log.Println(query)
				if err != nil {
					log.Println(err)
					continue
				}

				count := map[string][]ResultAggregateCount{}
				count[field] = valueCounts

				fieldCount := ResultAggregateFields{Count: count, TimeFrom: sanitizedQueryParams.From, TimeTo: sanitizedQueryParams.To, Total: total.Count}
				fieldCounts = append(fieldCounts, fieldCount)
			}
		}

	}

	return fieldCounts, total
}

// Returns total number of records for a given territory and series. Optional conditions for network, field/value, and date range. This is just a simple COUNT().
// However, since it accepts a date range, it could be called a few times to get a time series graph.
func (database *SocialHarvestDB) Count(queryParams CommonQueryParams, fieldValue string) ResultCount {
	sanitizedQueryParams := SanitizeCommonQueryParams(queryParams)
	var count = ResultCount{}

	if db.Postgres != nil {
		// The following query should work for pretty much any SQL database (at least any we're supporting)
		var err error

		var buffer bytes.Buffer
		buffer.WriteString("SELECT COUNT(*) AS count FROM ")
		buffer.WriteString(sanitizedQueryParams.Series)
		buffer.WriteString(" WHERE territory = '")
		buffer.WriteString(sanitizedQueryParams.Territory)
		buffer.WriteString("'")

		// optional date range (can have either or both)
		if sanitizedQueryParams.From != "" {
			buffer.WriteString(" AND time >= '")
			buffer.WriteString(sanitizedQueryParams.From)
			buffer.WriteString("'")
		}
		if sanitizedQueryParams.To != "" {
			buffer.WriteString(" AND time <= '")
			buffer.WriteString(sanitizedQueryParams.To)
			buffer.WriteString("'")
		}

		// Because we're accepting user inuput, use a prepared statement. Sanitizing fieldValue could also be done in the future perhaps (if needed).
		// The problem with prepared statements everywhere is that we can't put the tables through them. So only a few places will we be able to use them.
		// Here is one though.
		if sanitizedQueryParams.Field != "" && fieldValue != "" {
			buffer.WriteString(" AND ")
			buffer.WriteString(sanitizedQueryParams.Field)
			buffer.WriteString(" = $1")
		}

		// Again for the network
		if sanitizedQueryParams.Network != "" {
			buffer.WriteString(" AND network")
			// Must everything be so different?
			buffer.WriteString(" = $2")
		}

		query := buffer.String()
		buffer.Reset()
		if err != nil {
		}

		// TODO: There has to be a better way to do this.... Need to pass a variable number of args
		if fieldValue != "" && sanitizedQueryParams.Network == "" {
			err = db.Postgres.Get(&count, query, fieldValue)
		} else if fieldValue != "" && sanitizedQueryParams.Network != "" {
			err = db.Postgres.Get(&count, query, fieldValue, sanitizedQueryParams.Network)
		} else if fieldValue == "" && sanitizedQueryParams.Network != "" {
			err = db.Postgres.Get(&count, query, sanitizedQueryParams.Network)
		} else {
			err = db.Postgres.Get(&count, query)
		}

		count.TimeFrom = sanitizedQueryParams.From
		count.TimeTo = sanitizedQueryParams.To
	}

	return count
}

func (db *SocialHarvestDB) Messages(queryParams CommonQueryParams, conds MessageConditions) {

}

// // Allows the messages series to be queried in some general ways.
// func (database *SocialHarvestDB) Messages(queryParams CommonQueryParams, conds MessageConditions) ([]SocialHarvestMessage, uint64, uint, uint) {
// 	sanitizedQueryParams := SanitizeCommonQueryParams(queryParams)
// 	var results = []SocialHarvestMessage{}

// 	var err error

// 	// If one is even being used, connect to it and store the data
// 	sess, err = db.Open(dbAdapter, database.Settings)
// 	if err != nil {
// 		log.Println(err)
// 		return results, 0, sanitizedQueryParams.Skip, sanitizedQueryParams.Limit
// 	}
// 	defer sess.Close()

// 	col, err = sess.Collection("messages")
// 	if err != nil {
// 		log.Println(err)
// 		return results, 0, sanitizedQueryParams.Skip, sanitizedQueryParams.Limit
// 	}

// 	// optional date range (can have either or both)
// 	if sanitizedQueryParams.From != "" {
// 		switch database.Type {
// 		case "mongodb":
// 			conditions["time $gte"] = sanitizedQueryParams.From
// 			break
// 		default:
// 			conditions["time >="] = sanitizedQueryParams.From
// 			break
// 		}
// 	}
// 	if sanitizedQueryParams.To != "" {
// 		switch database.Type {
// 		case "mongodb":
// 			conditions["time $lte"] = sanitizedQueryParams.To
// 			break
// 		default:
// 			conditions["time <="] = sanitizedQueryParams.To
// 			break
// 		}
// 	}
// 	if sanitizedQueryParams.Network != "" {
// 		conditions["network"] = sanitizedQueryParams.Network
// 	}

// 	if sanitizedQueryParams.Territory != "" {
// 		conditions["territory"] = sanitizedQueryParams.Territory
// 	}

// 	// MessageConditions (specific conditions for messages series)
// 	if conds.Lang != "" {
// 		conditions["contributor_lang"] = conds.Lang
// 	}
// 	if conds.Country != "" {
// 		conditions["contributor_country"] = conds.Country
// 	}
// 	if conds.Geohash != "" {
// 		// Ensure the goehash is alphanumeric.
// 		// TODO: Pass these conditions through a sanitizer too, though the ORM should use prepared statements and take care of SQL injection....right? TODO: Check that too.
// 		pattern := `(?i)[A-z0-9]`
// 		r, _ := regexp.Compile(pattern)
// 		if r.MatchString(conds.Geohash) {
// 			switch database.Type {
// 			case "mongodb":
// 				conditions["contributor_geohash"] = "/" + conds.Geohash + ".*/"
// 				break
// 			default:
// 				conditions["contributor_geohash LIKE"] = conds.Geohash + "%"
// 				break
// 			}
// 		}
// 	}
// 	if conds.Gender != "" {
// 		switch conds.Gender {
// 		case "-1", "f", "female":
// 			conditions["contributor_gender"] = -1
// 			break
// 		case "1", "m", "male":
// 			conditions["contributor_gender"] = 1
// 			break
// 		case "0", "u", "unknown":
// 			conditions["contributor_gender"] = 0
// 			break
// 		}
// 	}
// 	if conds.IsQuestion != 0 {
// 		conditions["is_question"] = 1
// 	}

// 	// TODO: Allow other sorting options? I'm not sure it matters because people likely want timely data. More important would be a search.
// 	res := col.Find(conditions).Skip(sanitizedQueryParams.Skip).Limit(sanitizedQueryParams.Limit).Sort("-time")
// 	defer res.Close()
// 	total, resCountErr := res.Count()
// 	if resCountErr != nil {
// 		return results, 0, sanitizedQueryParams.Skip, sanitizedQueryParams.Limit
// 	}
// 	resErr := res.All(&results)
// 	if resErr != nil {
// 		return results, total, sanitizedQueryParams.Skip, sanitizedQueryParams.Limit
// 	}

// 	return results, total, sanitizedQueryParams.Skip, sanitizedQueryParams.Limit
// }
