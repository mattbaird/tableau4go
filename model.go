// Copyright 2013 Matthew Baird
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tableau4go

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"
)

const API_VERSION = "2.0"
const DEFAULT_SERVER = "http://localhost:8000"
const BOUNDARY_STRING = "813e3160-3c95-11e5-a151-feff819cdc9f"
const CRLF = "\r\n"

type API struct {
	Server              string
	Version             string
	Boundary            string
	AuthToken           string
	OmitDefaultSiteName bool
	DefaultSiteName     string
	ConnectTimeout      time.Duration
	ReadTimeout         time.Duration
	Debug               bool
}

func NewAPI(server string, version string, boundary string, defaultSiteName string, omitDefaultSiteName bool, cTimeout, rTimeout time.Duration) API {
	fixedUpServer := server
	if strings.HasSuffix(server, "/") {
		fixedUpServer = server[0 : len(server)-1]
	}
	return API{Server: fixedUpServer, Version: version, Boundary: boundary, DefaultSiteName: defaultSiteName, OmitDefaultSiteName: omitDefaultSiteName, ConnectTimeout: cTimeout, ReadTimeout: rTimeout}
}

type Project struct {
	ID          string `json:"id,omitempty" xml:"id,attr,omitempty"`
	Name        string `json:"name,omitempty" xml:"name,attr,omitempty"`
	Description string `json:"description,omitempty" xml:"description,attr,omitempty"`
}

// for sorting by tableau project name
type ProjectByName []Project

func (a ProjectByName) Len() int           { return len(a) }
func (a ProjectByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ProjectByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

type Projects struct {
	Projects []Project `json:"project,omitempty" xml:"project,omitempty"`
}

func NewProject(id string, name string, description string) Project {
	return Project{ID: id, Name: name, Description: description}
}

type CreateProjectResponse struct {
	Project Project `json:"project,omitempty" xml:"project,omitempty"`
}

type CreateProjectRequest struct {
	Request Project `json:"project,omitempty" xml:"project,omitempty"`
}

func (req CreateProjectRequest) XML() ([]byte, error) {
	tmp := struct {
		CreateProjectRequest
		XMLName struct{} `xml:"tsRequest"`
	}{CreateProjectRequest: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

func (p Project) XML() ([]byte, error) {
	return xml.MarshalIndent(p, "", "   ")
}

type DatasourceCreateRequest struct {
	Request Datasource `json:"datasource,omitempty" xml:"datasource,omitempty"`
}

func (req DatasourceCreateRequest) XML() ([]byte, error) {
	tmp := struct {
		DatasourceCreateRequest
		XMLName struct{} `xml:"tsRequest"`
	}{DatasourceCreateRequest: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

type Datasource struct {
	ID                    string                 `json:"id,omitempty" xml:"id,attr,omitempty"`
	Name                  string                 `json:"name,omitempty" xml:"name,attr,omitempty"`
	Type                  string                 `json:"type,omitempty" xml:"type,attr,omitempty"`
	ConnectionCredentials *ConnectionCredentials `json:"connectionCredentials,omitempty" xml:"connectionCredentials,omitempty"`
	Project               *Project               `json:"project,omitempty" xml:"project,omitempty"`
	Owner                 *User                  `json:"owner,omitempty" xml:"owner,omitempty"`
}

type Datasources struct {
	Datasources []Datasource `json:"datasource,omitempty" xml:"datasource,omitempty"`
}

type QueryDatasourcesResponse struct {
	Datasources Datasources `json:"datasources,omitempty" xml:"datasources,omitempty"`
}

type QueryDatasourceResponseRawXML struct {
	Datasource string `xml:",innerxml"`
}

func (ds *Datasource) XML() ([]byte, error) {
	return xml.MarshalIndent(ds, "", "   ")
}

type SigninRequest struct {
	Request Credentials `json:"credentials,omitempty" xml:"credentials,omitempty"`
}

func (req SigninRequest) XML() ([]byte, error) {
	tmp := struct {
		SigninRequest
		XMLName struct{} `xml:"tsRequest"`
	}{SigninRequest: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

type AuthResponse struct {
	Credentials *Credentials `json:"credentials,omitempty" xml:"credentials,omitempty"`
}

type ServerInfoResponse struct {
	ServerInfo ServerInfo `json:"serverInfo,omitempty" xml:"serverInfo,omitempty"`
}

type ServerInfo struct {
	ProductVersion string `json:"productVersion,omitempty" xml:"productVersion,omitempty"`
	RestApiVersion string `json:"restApiVersion,omitempty" xml:"restApiVersion,omitempty"`
}

type QueryProjectsResponse struct {
	Projects Projects `json:"projects,omitempty" xml:"projects,omitempty"`
}

type Credentials struct {
	Name        string `json:"name,omitempty" xml:"name,attr,omitempty"`
	Password    string `json:"password,omitempty" xml:"password,attr,omitempty"`
	Token       string `json:"token,omitempty" xml:"token,attr,omitempty"`
	Site        *Site  `json:"site,omitempty" xml:"site,omitempty"`
	Impersonate *User  `json:"user,omitempty" xml:"user,omitempty"`
}

type User struct {
	ID       string `json:"id,omitempty" xml:"id,attr,omitempty"`
	Name     string `json:"name,omitempty" xml:"name,attr,omitempty"`
	SiteRole string `json:"siteRole,omitempty" xml:"siteRole,attr,omitempty"`
	FullName string `json:"fullName,omitempty" xml:"fullName,attr,omitempty"`
}

type QuerySitesResponse struct {
	Sites Sites `json:"sites,omitempty" xml:"sites,omitempty"`
}

func (req QuerySitesResponse) XML() ([]byte, error) {
	tmp := struct {
		QuerySitesResponse
		XMLName struct{} `xml:"tsRequest"`
	}{QuerySitesResponse: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

type Sites struct {
	Sites []Site `json:"sites" xml:"sites,attr"`
}

type QuerySiteResponse struct {
	Site Site `json:"site,omitempty" xml:"site,omitempty"`
}

func (req QuerySiteResponse) XML() ([]byte, error) {
	tmp := struct {
		QuerySiteResponse
		XMLName struct{} `xml:"tsRequest"`
	}{QuerySiteResponse: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

type QueryUserOnSiteResponse struct {
	User User `json:"user,omitempty" xml:"user,omitempty"`
}

func (req QueryUserOnSiteResponse) XML() ([]byte, error) {
	tmp := struct {
		QueryUserOnSiteResponse
		XMLName struct{} `xml:"tsRequest"`
	}{QueryUserOnSiteResponse: req}
	return xml.MarshalIndent(tmp, "", "   ")
}

type Site struct {
	ID           string     `json:"id,omitempty" xml:"id,attr,omitempty"`
	Name         string     `json:"name,omitempty" xml:"name,attr,omitempty"`
	ContentUrl   string     `json:"contentUrl,omitempty" xml:"contentUrl,attr,omitempty"`
	AdminMode    string     `json:"adminMode,omitempty" xml:"adminMode,attr,omitempty"`
	UserQuota    string     `json:"userQuota,omitempty" xml:"userQuota,attr,omitempty"`
	StorageQuota int        `json:"storageQuota,omitempty" xml:"storageQuota,attr,omitempty"`
	State        string     `json:"state,omitempty" xml:"state,attr,omitempty"`
	StatusReason string     `json:"statusReason,omitempty" xml:"statusReason,attr,omitempty"`
	Usage        *SiteUsage `json:"usage,omitempty" xml:"usage,omitempty"`
}

type SiteUsage struct {
	NumberOfUsers int `json:"number-of-users" xml:"number-of-users,attr"`
	Storage       int `json:"storage" xml:"storage,attr"`
}

type ConnectionCredentials struct {
	Name     string `json:"name,omitempty" xml:"name,attr,omitempty"`
	Password string `json:"password,omitempty" xml:"password,attr,omitempty"`
	Embed    bool   `json:"embed" xml:"embed,attr"`
}

func NewConnectionCredentials(name, password string, embed bool) ConnectionCredentials {
	return ConnectionCredentials{Name: name, Password: password, Embed: embed}
}

type ErrorResponse struct {
	Error Terror `json:"error,omitempty" xml:"error,omitempty"`
}

type Terror struct {
	Code    string `json:"code,omitempty" xml:"code,attr,omitempty"`
	Summary string `json:"summary,omitempty" xml:"summary,omitempty"`
	Detail  string `json:"detail,omitempty" xml:"detail,omitempty"`
}

func (t Terror) Error() string {
	return fmt.Sprintf("Code:%s, Summary:%s, Detail:%s", t.Code, t.Summary, t.Detail)
}

type StatusError struct {
	Code int
	Msg  string
	URL  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%d - %s.  Request URL was: %s", e.Code, e.Msg, e.URL)
}
