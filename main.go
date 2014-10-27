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

package main

import (
	"encoding/json"
	"flag"
	"github.com/SocialHarvest/harvester/lib/config"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/bugsnag/bugsnag-go"
	"github.com/fatih/color"
	"log"
	"net/http"
	//_ "net/http/pprof"
	"os"
	//"runtime"
	"strconv"
)

var socialHarvest = config.SocialHarvest{}

// --------- API Basic Auth Middleware (valid keys are defined in the Social Harvest config, there are no roles or anything)
type BasicAuthMw struct {
	Realm string
	Key   string
}

func (bamw *BasicAuthMw) MiddlewareFunc(handler rest.HandlerFunc) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {

		authHeader := r.Header.Get("Authorization")
		log.Println(authHeader)
		if authHeader == "" {
			queryParams := r.URL.Query()
			if len(queryParams["apiKey"]) > 0 {
				bamw.Key = queryParams["apiKey"][0]
			} else {
				bamw.unauthorized(w)
				return
			}
		} else {
			bamw.Key = authHeader
		}

		keyFound := false
		for _, key := range socialHarvest.Config.ReporterServer.AuthKeys {
			if bamw.Key == key {
				keyFound = true
			}
		}

		if !keyFound {
			bamw.unauthorized(w)
			return
		}

		handler(w, r)
	}
}

func (bamw *BasicAuthMw) unauthorized(w rest.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Basic realm="+bamw.Realm)
	rest.Error(w, "Not Authorized", http.StatusUnauthorized)
}

// Main - initializes, configures, and sets routes for API
func main() {
	appVersion := "0.1.0-preview"

	// Optionally allow a config JSON file to be passed via command line
	var confFile string
	flag.StringVar(&confFile, "conf", "social-harvest-conf.json", "Path to the Social Harvest configuration file.")
	flag.Parse()

	// Open the config JSON and decode it.
	file, _ := os.Open(confFile)
	decoder := json.NewDecoder(file)
	configuration := config.SocialHarvestConf{}
	err := decoder.Decode(&configuration)
	if err != nil {
		log.Println("error:", err)
	}

	// Set the configuration, DB client, etc. so that it is available to other stuff.
	socialHarvest.Config = configuration

	// Setup Bugsnag (first), profiling, etc.
	if socialHarvest.Config.Debug.Bugsnag.ApiKey != "" {
		bugsnag.Configure(bugsnag.Configuration{
			APIKey:          socialHarvest.Config.Debug.Bugsnag.ApiKey,
			ReleaseStage:    socialHarvest.Config.Debug.Bugsnag.ReleaseStage,
			ProjectPackages: []string{"main", "github.com/SocialHarvest/reporter/*"},
			AppVersion:      appVersion,
		})
	}

	// Debug - do not compile with this
	// runtime.SetBlockProfileRate(1)
	// // Start a profile server so information can be viewed using a web browser
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	// Banner (would appear twice if it came before bugsnag for some reason)
	color.Cyan(" ____             _       _   _   _                           _  ")
	color.Cyan(`/ ___|  ___   ___(_) __ _| | | | | | __ _ _ ____   _____  ___| |_ ®`)
	color.Cyan("\\___ \\ / _ \\ / __| |/ _` | | | |_| |/ _` | '__\\ \\ / / _ \\/ __| __|")
	color.Cyan(" ___) | (_) | (__| | (_| | | |  _  | (_| | |   \\ V /  __/\\__ \\ |_ ")
	color.Cyan("|____/ \\___/ \\___|_|\\__,_|_| |_| |_|\\__,_|_|    \\_/ \\___||___/\\__|")
	//	color.Cyan("                                                                  ")
	color.Yellow("__________________________________(reporter) version " + appVersion)
	color.Cyan("   ")

	// Continue configuration
	newDatabase(socialHarvest.Config)
	if db.Postgres != nil {
		defer db.Postgres.Close()
	}

	// The RESTful API reporter server can be completely disabled by setting {"reporterServer":{"disabled": true}} in the config
	// TODO: Think about accepting command line arguments for reporting/exporting.
	if !socialHarvest.Config.ReporterServer.Disabled {
		restMiddleware := []rest.Middleware{}

		// If additional origins were allowed for CORS, handle them
		if len(socialHarvest.Config.ReporterServer.Cors.AllowedOrigins) > 0 {
			restMiddleware = append(restMiddleware,
				&rest.CorsMiddleware{
					RejectNonCorsRequests: false,
					OriginValidator: func(origin string, r *rest.Request) bool {
						for _, allowedOrigin := range socialHarvest.Config.ReporterServer.Cors.AllowedOrigins {
							// If the request origin matches one of the allowed origins, return true
							if origin == allowedOrigin {
								return true
							}
						}
						return false
					},
					AllowedMethods: []string{"GET", "POST", "PUT"},
					AllowedHeaders: []string{
						"Accept", "Content-Type", "X-Custom-Header", "Origin"},
					AccessControlAllowCredentials: true,
					AccessControlMaxAge:           3600,
				},
			)
		}
		// If api keys are defined, setup basic auth (any key listed allows full access, there are no roles for now, this is just very basic auth)
		if len(socialHarvest.Config.ReporterServer.AuthKeys) > 0 {
			restMiddleware = append(restMiddleware,
				&BasicAuthMw{
					Realm: "Social Harvest (reporter) API",
					Key:   "",
				},
			)
		}

		handler := rest.ResourceHandler{
			EnableRelaxedContentType: true,
			PreRoutingMiddlewares:    restMiddleware,
		}
		err := handler.SetRoutes(
			&rest.Route{"GET", "/", TopLocations},

			&rest.Route{"GET", "/database/info", DatabaseInfo},
			&rest.Route{"GET", "/territory/list", TerritoryList},
			&rest.Route{"GET", "/link/details", LinkDetails},

			&rest.Route{"GET", "/territory/aggregate/:territory/:series", TerritoryAggregateData},
			// Simple counts for a territory
			&rest.Route{"GET", "/territory/count/:territory/:series/:field", TerritoryCountData},
			&rest.Route{"GET", "/territory/timeseries/count/:territory/:series/:field", TerritoryTimeseriesCountData},
			// Messages for a territory
			&rest.Route{"GET", "/territory/messages/:territory", TerritoryMessages},
		)
		if err != nil {
			log.Fatal(err)
		}

		// Allow the port to be configured (we need it as a string, but let the config define an int)
		p := strconv.Itoa(socialHarvest.Config.ReporterServer.Port)
		// But if it can't be parsed (maybe wasn't set) then set it to 3001
		if p == "0" {
			p = "3001"
		}
		log.Println("Social Harvest (harvester) API listening on port " + p)
		if socialHarvest.Config.Debug.Bugsnag.ApiKey != "" {
			log.Println(http.ListenAndServe(":"+p, bugsnag.Handler(&handler)))
		} else {
			log.Fatal(http.ListenAndServe(":"+p, &handler))
		}
	}
}
