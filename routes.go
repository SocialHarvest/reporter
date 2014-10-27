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

// This file contains all the functions to handle the API routes.
package main

import (
	"github.com/SocialHarvest/harvester/lib/config"
	"github.com/advancedlogic/GoOse"
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Returns information about the currently configured database, if it's reachable, etc.
func DatabaseInfo(w rest.ResponseWriter, r *rest.Request) {
	res := config.NewHypermediaResource()
	res.Links["database:info"] = config.HypermediaLink{
		Href: "/database/info",
	}

	if db.Postgres != nil {
		res.Data["type"] = "postgres"
		// SELECT * FROM has_database_privilege('username', 'database', 'connect');
		// var r struct {
		// 	hasAccess string `db:"has_database_privilege" json:"has_database_privilege"`
		// }
		//err := socialHarvest.Database.Postgres.Get(&r, "SELECT * FROM has_database_privilege("+socialHarvest.Config.Database.User+", "+socialHarvest.Config.Database.Database+", 'connect')")
		//res.Data["r"] = r
		//res.Data["err"] = err
		res.Data["hasAccess"] = db.HasAccess()
	}
	if db.InfluxDB != nil {
		res.Data["type"] = "infxludb"
	}

	res.Success()
	w.WriteJson(res.End())
}

// Returns the top locations for a given territory
func TopLocations(w rest.ResponseWriter, r *rest.Request) {
	res := config.NewHypermediaResource()

	res.Data["foo"] = "bar"

	//db.Top()

	res.Success()
	w.WriteJson(res.End("Some message."))
}

// Territory aggregates (gender, language, etc.) shows a breakdown and count of various values and their percentage of total
func TerritoryAggregateData(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:aggregate")
	res.Links["self"] = config.HypermediaLink{
		Href: "/territory/aggregate/{territory}/{series}{?from,to,fields,network}",
	}
	res.Links["territory:list"] = config.HypermediaLink{
		Href: "/territory/list",
	}
	res.Links["territory:timeseries-aggregate"] = config.HypermediaLink{
		Href: "/territory/timeseries/aggregate/{territory}/{series}{?from,to,fields,network,resolution}",
	}

	territory := r.PathParam("territory")
	series := r.PathParam("series")
	queryParams := r.URL.Query()

	timeFrom := ""
	if len(queryParams["from"]) > 0 {
		timeFrom = queryParams["from"][0]
	}
	timeTo := ""
	if len(queryParams["to"]) > 0 {
		timeTo = queryParams["to"][0]
	}
	network := ""
	if len(queryParams["network"]) > 0 {
		timeTo = queryParams["network"][0]
	}

	limit := 0
	if len(queryParams["limit"]) > 0 {
		parsedLimit, err := strconv.Atoi(queryParams["limit"][0])
		if err == nil {
			limit = parsedLimit
		}
	}

	fields := []string{}
	if len(queryParams["fields"]) > 0 {
		fields = strings.Split(queryParams["fields"][0], ",")
		// trim any white space
		for i, val := range fields {
			fields[i] = strings.Trim(val, " ")
		}
	}

	if territory != "" && series != "" && len(fields) > 0 {
		params := CommonQueryParams{
			Series:    series,
			Territory: territory,
			Network:   network,
			From:      timeFrom,
			To:        timeTo,
			Limit:     uint(limit),
		}

		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields)
		res.Data["total"] = total.Count
		res.Success()
	}

	w.WriteJson(res.End())
}

// Returns a simple count based on various conditions.
func TerritoryCountData(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:count")

	territory := r.PathParam("territory")
	series := r.PathParam("series")
	field := r.PathParam("field")
	queryParams := r.URL.Query()

	timeFrom := ""
	if len(queryParams["from"]) > 0 {
		timeFrom = queryParams["from"][0]
	}
	timeTo := ""
	if len(queryParams["to"]) > 0 {
		timeTo = queryParams["to"][0]
	}
	fieldValue := ""
	if len(queryParams["fieldValue"]) > 0 {
		fieldValue = queryParams["fieldValue"][0]
	}
	network := ""
	if len(queryParams["network"]) > 0 {
		network = queryParams["network"][0]
	}

	params := CommonQueryParams{
		Series:    series,
		Territory: territory,
		Field:     field,
		Network:   network,
		From:      timeFrom,
		To:        timeTo,
	}

	var count ResultCount
	count = db.Count(params, fieldValue)
	res.Data["count"] = count.Count
	res.Meta.From = count.TimeFrom
	res.Meta.To = count.TimeTo

	res.Success()
	w.WriteJson(res.End())
}

// Returns a simple count based on various conditions in a streaming time series.
func TerritoryTimeseriesCountData(w rest.ResponseWriter, r *rest.Request) {
	territory := r.PathParam("territory")
	series := r.PathParam("series")
	field := r.PathParam("field")
	queryParams := r.URL.Query()

	timeFrom := ""
	if len(queryParams["from"]) > 0 {
		timeFrom = queryParams["from"][0]
	}
	timeTo := ""
	if len(queryParams["to"]) > 0 {
		timeTo = queryParams["to"][0]
	}
	fieldValue := ""
	if len(queryParams["fieldValue"]) > 0 {
		fieldValue = queryParams["fieldValue"][0]
	}
	network := ""
	if len(queryParams["network"]) > 0 {
		network = queryParams["network"][0]
	}

	params := CommonQueryParams{
		Series:    series,
		Territory: territory,
		Field:     field,
		Network:   network,
		From:      timeFrom,
		To:        timeTo,
	}

	// in minutes
	resolution := 0
	if len(queryParams["resolution"]) > 0 {
		parsedResolution, err := strconv.Atoi(queryParams["resolution"][0])
		if err == nil {
			resolution = parsedResolution
		}
	}

	if resolution != 0 && territory != "" && series != "" {
		// only accepting days for now - not down to minutes or hours (yet)
		tF, _ := time.Parse("2006-01-02", timeFrom)
		tT, _ := time.Parse("2006-01-02", timeTo)

		timeRange := tT.Sub(tF)
		//totalRangeMinutes := int(timeRange.Minutes())
		periodsInRange := int(timeRange.Minutes() / float64(resolution))

		w.Header().Set("Content-Type", "application/json")
		var count ResultCount
		for i := 0; i < periodsInRange; i++ {
			params.From = tF.Format("2006-01-02 15:04:05")
			tF = tF.Add(time.Duration(resolution) * time.Minute)
			params.To = tF.Format("2006-01-02 15:04:05")

			count = db.Count(params, fieldValue)
			w.WriteJson(count)
			w.(http.ResponseWriter).Write([]byte("\n"))
			// Flush the buffer to client immediately
			// (for most cases, this stream will be quick and short - just how we like it. for the more crazy requests, it may take a little while and that's ok too)
			w.(http.Flusher).Flush()
		}
	}

}

// API: Returns the messages (paginated) for a territory with the ability to filter by question or not, etc.
func TerritoryMessages(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:messages")

	territory := r.PathParam("territory")
	queryParams := r.URL.Query()

	timeFrom := ""
	if len(queryParams["from"]) > 0 {
		timeFrom = queryParams["from"][0]
	}
	timeTo := ""
	if len(queryParams["to"]) > 0 {
		timeTo = queryParams["to"][0]
	}
	network := ""
	if len(queryParams["network"]) > 0 {
		network = queryParams["network"][0]
	}
	// Limit and Skip
	limit := uint(100)
	if len(queryParams["limit"]) > 0 {
		l, lErr := strconv.ParseUint(queryParams["limit"][0], 10, 64)
		if lErr == nil {
			limit = uint(l)
		}
		if limit > 100 {
			limit = 100
		}
		if limit < 1 {
			limit = 1
		}
	}
	skip := uint(0)
	if len(queryParams["skip"]) > 0 {
		sk, skErr := strconv.ParseUint(queryParams["skip"][0], 10, 64)
		if skErr == nil {
			skip = uint(sk)
		}
		if skip < 0 {
			skip = 0
		}
	}

	// Build the conditions
	var conditions = MessageConditions{}

	// Condition for questions
	if len(queryParams["questions"]) > 0 {
		conditions.IsQuestion = 1
	}
	// Gender condition
	if len(queryParams["gender"]) > 0 {
		conditions.Gender = queryParams["gender"][0]
	}
	// Language condition
	if len(queryParams["lang"]) > 0 {
		conditions.Lang = queryParams["lang"][0]
	}
	// Country condition
	if len(queryParams["country"]) > 0 {
		conditions.Country = queryParams["country"][0]
	}
	// Geohash condition (nearby)
	if len(queryParams["geohash"]) > 0 {
		conditions.Geohash = queryParams["geohash"][0]
	}

	params := CommonQueryParams{
		Series:    "messages",
		Territory: territory,
		Network:   network,
		From:      timeFrom,
		To:        timeTo,
		Limit:     limit,
		Skip:      skip,
	}

	//messages, total, skip, limit := db.Messages(params, conditions)
	// res.Data["messages"] = messages
	// res.Data["total"] = total
	// res.Data["limit"] = limit
	// res.Data["skip"] = skip
	db.Messages(params, conditions)

	res.Success()
	w.WriteJson(res.End())

}

// Returns all currently configured territories and their settings
func TerritoryList(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:list")
	res.Data["territories"] = socialHarvest.Config.Harvest.Territories
	res.Success()
	w.WriteJson(res.End())
}

// Sets the hypermedia response "_links" section with all of the routes we have defined for territories.
func setTerritoryLinks(self string) *config.HypermediaResource {
	res := config.NewHypermediaResource()
	res.Links["territory:list"] = config.HypermediaLink{
		Href: "/territory/list",
	}
	res.Links["territory:count"] = config.HypermediaLink{
		Href: "/territory/count/{territory}/{series}/{field}{?from,to,network,fieldValue}",
	}
	res.Links["territory:timeseries-count"] = config.HypermediaLink{
		Href: "/territory/timeseries/count/{territory}/{series}/{field}{?from,to,network,fieldValue}",
	}
	res.Links["territory:aggregate"] = config.HypermediaLink{
		Href: "/territory/aggregate/{territory}/{series}{?from,to,network,fields}",
	}
	res.Links["territory:timeseries-aggregate"] = config.HypermediaLink{
		Href: "/territory/timeseries/aggregate/{territory}/{series}{?from,to,network,fields,resolution}",
	}
	res.Links["territory:messages"] = config.HypermediaLink{
		Href: "/territory/messages/{territory}{?from,to,limit,skip,network,lang,country,geohash,gender,questions}",
	}

	selfedRes := config.NewHypermediaResource()
	for link, _ := range res.Links {
		if link == self {
			selfedRes.Links["self"] = res.Links[link]
		} else {
			selfedRes.Links[link] = res.Links[link]
		}
	}
	return selfedRes
}

// --------- Utility API end points ---------

// Retrieves information to provide a summary about a give URL, specifically articles/blog posts.
// TODO: Make this more robust (more details, videos, etc.). Some of this may eventually also go into the harvest.
// TODO: Likely fork this package and add in some of the things I did for Virality Score in order to get even more data.
func LinkDetails(w rest.ResponseWriter, r *rest.Request) {
	res := config.NewHypermediaResource()
	res.Links["self"] = config.HypermediaLink{
		Href: "/link/details{?url}",
	}

	queryParams := r.URL.Query()
	if len(queryParams["url"]) > 0 {
		g := goose.New()
		article := g.ExtractFromUrl(queryParams["url"][0])

		res.Data["title"] = article.Title
		res.Data["published"] = article.PublishDate
		res.Data["favicon"] = article.MetaFavicon
		res.Data["domain"] = article.Domain
		res.Data["description"] = article.MetaDescription
		res.Data["keywords"] = article.MetaKeywords
		res.Data["content"] = article.CleanedText
		res.Data["url"] = article.FinalUrl
		res.Data["image"] = article.TopImage
		res.Data["movies"] = article.Movies
		res.Success()
	}

	w.WriteJson(res.End())
}
