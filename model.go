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
	"time"
)

var (
	connectTimeOut   = time.Duration(30 * time.Second)
	readWriteTimeout = time.Duration(30 * time.Second)
)

const API_VERSION = "2.0"
const DEFAULT_SERVER = "http://localhost:8000"
const BOUNDARY_STRING = "813e3160-3c95-11e5-a151-feff819cdc9f"
const CRLF = "\r\n"

type API struct {
	Server    string
	Version   string
	Boundary  string
	AuthToken string
}

func DefaultApi() API {
	return API{Server: DEFAULT_SERVER, Version: API_VERSION, Boundary: BOUNDARY_STRING}
}

func NewAPI(server string, version string, boundary string) API {
	return API{Server: server, Version: version, Boundary: boundary}
}

type Project struct {
	ID          string `json:"id,omitempty" xml:"id,attr,omitempty"`
	Name        string `json:"name,omitempty" xml:"name,attr,omitempty"`
	Description string `json:"description,omitempty" xml:"description,attr,omitempty"`
}
type Projects struct {
	Projects []Project `json:"project,omitempty" xml:"project,omitempty"`
}

func NewProject(id string, name string, description string) Project {
	return Project{ID: id, Name: name, Description: description}
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
	Name                  string                 `json:"name,omitempty" xml:"name,attr,omitempty"`
	ConnectionCredentials *ConnectionCredentials `json:"connectionCredentials,omitempty" xml:"connectionCredentials,omitempty"`
	Project               *Project               `json:"project,omitempty" xml:"project,omitempty"`
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
	ID string `json:"id,omitempty" xml:"id,attr,omitempty"`
}

type Site struct {
	ContentUrl string `json:"contentUrl" xml:"contentUrl,attr"`
}
type ConnectionCredentials struct {
	Name     string `json:"name,omitempty" xml:"name,attr,omitempty"`
	Password string `json:"password,omitempty" xml:"password,attr,omitempty"`
	Embed    bool   `json:"embed" xml:"embed,attr"`
}

func (ds *Datasource) XML() ([]byte, error) {
	return xml.MarshalIndent(ds, "", "   ")
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
