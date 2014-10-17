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
)

// Returns the top locations for a given territory
func TopLocations(w rest.ResponseWriter, r *rest.Request) {
	res := config.NewHypermediaResource()

	res.Data["foo"] = "bar"

	res.Success()
	w.WriteJson(res.End("Some message."))
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
