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
	"bytes"
	"github.com/SocialHarvest/harvester/lib/config"
	"github.com/advancedlogic/GoOse"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
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

// Territory aggregates (gender, language, etc.) shows a breakdown and count of various values and their percentage of total
func TerritoryAggregateData(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:aggregate")

	params, fields, extraParams := buildAggregateParams(r)

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
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
	// Limit and Skip
	limit := uint64(100)
	if len(queryParams["limit"]) > 0 {
		l, lErr := strconv.ParseUint(queryParams["limit"][0], 10, 64)
		if lErr == nil {
			limit = uint64(l)
		}
		if limit > 100 {
			limit = 100
		}
		if limit < 1 {
			limit = 1
		}
	}
	skip := uint64(0)
	if len(queryParams["skip"]) > 0 {
		sk, skErr := strconv.ParseUint(queryParams["skip"][0], 10, 64)
		if skErr == nil {
			skip = uint64(sk)
		}
		if skip < 0 {
			skip = 0
		}
	}

	params := CommonQueryParams{
		Series:    series,
		Territory: territory,
		Field:     field,
		Network:   network,
		From:      timeFrom,
		To:        timeTo,
		Skip:      skip,
		Limit:     limit,
	}

	var count ResultCount
	count = db.Count(params, fieldValue)
	res.Data["count"] = count.Count
	res.Data["limit"] = limit
	res.Data["skip"] = skip
	res.Meta.From = count.TimeFrom
	res.Meta.To = count.TimeTo

	res.Success()
	w.WriteJson(res.End())
}

// Returns the top images for a given territory
func TerritoryTopImages(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-images")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"expanded_url"}
	// same with the series
	params.Series = "shared_links"
	// special params
	extraParams["type"] = " IN('photo','image')"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top videos for a given territory
func TerritoryTopVideos(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-videos")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"expanded_url"}
	// same with the series
	params.Series = "shared_links"
	// special params
	extraParams["type"] = " = 'video'"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top audio for a given territory
func TerritoryTopAudio(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-audio")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"expanded_url"}
	// same with the series
	params.Series = "shared_links"
	// special params
	extraParams["type"] = " = 'audio'"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top non video/image/audio links for a given territory
func TerritoryTopLinks(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-links")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"expanded_url"}
	// same with the series
	params.Series = "shared_links"
	// special params
	extraParams["type"] = " = ''"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top keywords for a given territory (primarily a convenience route for a simple aggregate, also makes use of LOWER())
func TerritoryTopKeywords(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-keywords")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"LOWER(keyword)"}
	// same with the series
	params.Series = "hashtags"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top hashtags for a given territory (primarily a convenience route for a simple aggregate, also makes use of LOWER())
func TerritoryTopHashtags(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-hashtags")

	params, fields, extraParams := buildAggregateParams(r)
	// override, we know the field we want and its just one in this case
	fields = []string{"LOWER(tag)"}
	// same with the series
	params.Series = "hashtags"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

	w.WriteJson(res.End())
}

// Returns the top locations for a given territory
func TerritoryTopLocations(w rest.ResponseWriter, r *rest.Request) {
	res := setTerritoryLinks("territory:top-locations")

	queryParams := r.URL.Query()
	params, fields, extraParams := buildAggregateParams(r)
	// override the fields, we know the field we want and its just one in this case ... but with an optional precision value
	precision := 7
	var err error
	if len(queryParams["precision"]) > 0 {
		precision, err = strconv.Atoi(queryParams["precision"][0])
		if err != nil {
			precision = 7
		}
	}
	// Keep it within the limits
	if precision > 12 {
		precision = 12
	}
	if precision < 1 {
		precision = 1
	}
	var buffer bytes.Buffer
	buffer.WriteString("substring(contributor_geohash, 1,")
	buffer.WriteString(strconv.Itoa(precision))
	buffer.WriteString(")")
	geohash := buffer.String()
	buffer.Reset()
	fields = []string{geohash}
	// same with the series
	params.Series = "messages"

	if params.Territory != "" && params.Series != "" && len(fields) > 0 {
		var total ResultCount
		res.Data["aggregate"], total = db.FieldCounts(params, fields, extraParams)
		res.Data["total"] = total.Count
		res.Success()
	} else {
		res.Data["aggregate"] = nil
		res.Data["total"] = 0
	}

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
	limit := uint64(100)
	if len(queryParams["limit"]) > 0 {
		l, lErr := strconv.ParseUint(queryParams["limit"][0], 10, 64)
		if lErr == nil {
			limit = uint64(l)
		}
		if limit > 100 {
			limit = 100
		}
		if limit < 1 {
			limit = 1
		}
	}
	skip := uint64(0)
	if len(queryParams["skip"]) > 0 {
		sk, skErr := strconv.ParseUint(queryParams["skip"][0], 10, 64)
		if skErr == nil {
			skip = uint64(sk)
		}
		if skip < 0 {
			skip = 0
		}
	}

	// Build the conditions
	var conditions = BasicConditions{}

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

	messages, total, skip, limit := db.Messages(params, conditions)
	res.Data["messages"] = messages
	res.Data["total"] = total
	res.Data["limit"] = limit
	res.Data["skip"] = skip
	//db.Messages(params, conditions)

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
	res.Links["territory:top-images"] = config.HypermediaLink{
		Href: "/territory/top/images/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-videos"] = config.HypermediaLink{
		Href: "/territory/top/videos/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-audio"] = config.HypermediaLink{
		Href: "/territory/top/audio/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-links"] = config.HypermediaLink{
		Href: "/territory/top/links/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-locations"] = config.HypermediaLink{
		Href: "/territory/top/locations/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-keywords"] = config.HypermediaLink{
		Href: "/territory/top/keywords/{territory}/{series}{?from,to,network}",
	}
	res.Links["territory:top-hashtags"] = config.HypermediaLink{
		Href: "/territory/top/hashtags/{territory}/{series}{?from,to,network}",
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
		log.Println(article)

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

func buildAggregateParams(r *rest.Request) (CommonQueryParams, []string, map[string]string) {
	territory := r.PathParam("territory")
	series := r.PathParam("series")
	queryParams := r.URL.Query()
	extraParams := make(map[string]string)
	params := CommonQueryParams{}
	var err error

	// Fields to group by
	fields := []string{}
	if len(queryParams["fields"]) > 0 {
		fields = strings.Split(queryParams["fields"][0], ",")
		// trim any white space
		for i, val := range fields {
			fields[i] = strings.Trim(val, " ")
		}
	}

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
		} else {
			log.Println("Error parsing limit param:")
			log.Println(err)
		}
	}
	skip := 0
	if len(queryParams["skip"]) > 0 {
		parsedSkip, skipErr := strconv.Atoi(queryParams["skip"][0])
		if skipErr == nil {
			skip = parsedSkip
		} else {
			log.Println("Error parsing skip param:")
			log.Println(err)
		}
	}

	params.Series = series
	params.Territory = territory
	params.Network = network
	params.From = timeFrom
	params.To = timeTo
	params.Limit = uint64(limit)
	params.Skip = uint64(skip)

	return params, fields, extraParams
}
