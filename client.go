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
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/AtScaleInc/apps-shared/httputil"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (api *API) Signin(username, password string, contentUrl string, userIdToImpersonate string) error {
	path := fmt.Sprintf("/api/%s/auth/signin", api.Version)
	url := api.Server + path
	credentials := Credentials{Name: username, Password: password}
	if len(userIdToImpersonate) > 0 {
		credentials.Impersonate = &User{ID: userIdToImpersonate}
	}
	credentials.Site = &Site{ContentUrl: contentUrl}
	request := SigninRequest{Request: credentials}
	signInXML, err := request.XML()
	if err != nil {
		return err
	}
	payload := string(signInXML)
	headers := make(map[string]string)
	headers["Content-Type"] = "application/xml"
	retval := AuthResponse{}
	err = api.makeRequest(url, "POST", []byte(payload), &retval, headers, connectTimeOut, readWriteTimeout)
	if err == nil {
		api.AuthToken = retval.Credentials.Token
	}
	return err
}

func (api *API) Signout() error {
	path := fmt.Sprintf("/api/%s/auth/signout", api.Version)
	url := api.Server + path
	headers := make(map[string]string)
	headers["Content-Type"] = "application/xml"
	headers["X-Tableau-Auth"] = api.AuthToken
	err := api.makeRequest(url, "POST", []byte{}, nil, headers, connectTimeOut, readWriteTimeout)
	return err
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Query_Projects%3FTocPath%3DAPI%2520Reference%7C_____38
func (api *API) ListProjects(siteId string) ([]Project, error) {
	path := fmt.Sprintf("/api/%s/sites/%s/projects", api.Version, siteId)
	url := api.Server + path
	headers := make(map[string]string)
	headers["X-Tableau-Auth"] = api.AuthToken
	retval := QueryProjectsResponse{}
	err := api.makeRequest(url, "GET", []byte(""), &retval, headers, connectTimeOut, readWriteTimeout)
	return retval.Projects.Projects, err
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Create_Project%3FTocPath%3DAPI%2520Reference%7C_____14
//POST /api/api-version/sites/site-id/projects
func (api *API) CreateProject(siteId string, project Project) (retval *Project, err error) {
	path := fmt.Sprintf("/api/%s/sites/%s/projects", api.Version, siteId)
	url := api.Server + path
	createProjectRequest := CreateProjectRequest{Request: project}
	xmlRep, err := createProjectRequest.XML()
	if err != nil {
		return &project, err
	}
	headers := make(map[string]string)
	headers["X-Tableau-Auth"] = api.AuthToken
	headers["Content-Type"] = "application/xml"
	err = api.makeRequest(url, "POST", xmlRep, retval, headers, connectTimeOut, readWriteTimeout)
	if err != nil {
		fmt.Printf("error creating project:%v\n", err)
	}
	return retval, err
}

//http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_ref.htm#Publish_Datasource%3FTocPath%3DAPI%2520Reference%7C_____31
//POST /api/api-version/sites/site-id/datasources?overwrite=overwrite-flag
func (api *API) PublishDataSource(siteId string, tdsMetadata Datasource, fullTds string, overwrite bool) (retval *Datasource, err error) {
	path := fmt.Sprintf("/api/%s/sites/%s/datasources?datasourceType=%s&overwrite=%v", api.Version, siteId, "tds", overwrite)
	url := api.Server + path
	payload := fmt.Sprintf("--%s\r\n", api.Boundary)
	payload += "Content-Disposition: name=\"request_payload\"\r\n"
	payload += "Content-Type: text/xml\r\n"
	payload += "\r\n"
	tdsMetadata.Project.ID = "6bc04090-0b2e-448a-8c00-b3bbe6352b11"
	tdsRequest := DatasourceCreateRequest{Request: tdsMetadata}
	xmlRepresentation, err := tdsRequest.XML()
	if err != nil {
		return retval, err
	}
	payload += string(xmlRepresentation)
	payload += fmt.Sprintf("\r\n--%s\r\n", api.Boundary)
	payload += fmt.Sprintf("Content-Disposition: name=\"tableau_datasource\"; filename=\"%s.tds\"\r\n", tdsMetadata.Name)
	payload += "Content-Type: application/octet-stream\r\n"
	payload += "\r\n"
	payload += fullTds
	payload += fmt.Sprintf("\r\n--%s--\r\n", api.Boundary)
	headers := make(map[string]string)
	headers["Content-Type"] = fmt.Sprintf("multipart/mixed; boundary=%s", api.Boundary)
	headers["Content-Length"] = fmt.Sprintf("%v", len([]byte(payload)))
	headers["X-Tableau-Auth"] = api.AuthToken
	err = api.makeRequest(url, "POST", []byte(payload), retval, headers, connectTimeOut, readWriteTimeout)
	return retval, err
}

func (api *API) makeRequest(requestUrl string, method string, payload []byte, result interface{}, headers map[string]string,
	cTimeout time.Duration, rwTimeout time.Duration) error {
	client := httputil.NewTimeoutClient(cTimeout, rwTimeout)
	var req *http.Request
	if len(payload) > 0 {
		var httpErr error
		req, httpErr = http.NewRequest(strings.TrimSpace(method), strings.TrimSpace(requestUrl), bytes.NewBuffer(payload))
		if httpErr != nil {
			return httpErr
		}
		req.Header.Add("Content-Length", strconv.Itoa(len(payload)))
	} else {
		var httpErr error
		req, httpErr = http.NewRequest(strings.TrimSpace(method), strings.TrimSpace(requestUrl), nil)
		if httpErr != nil {
			return httpErr
		}
	}
	if headers != nil {
		for header, headerValue := range headers {
			req.Header.Add(header, headerValue)
		}
	}
	var httpErr error
	resp, httpErr := client.Do(req)
	if httpErr != nil {
		return httpErr
	}
	defer resp.Body.Close()
	body, readBodyError := ioutil.ReadAll(resp.Body)
	if readBodyError != nil {
		return readBodyError
	}
	if resp.StatusCode == 404 {
		return fmt.Errorf("Not Found")
	}
	if resp.StatusCode >= 300 {
		tErrorResponse := ErrorResponse{}
		err := xml.Unmarshal(body, &tErrorResponse)
		if err != nil {
			return err
		}
		return tErrorResponse.Error
	}
	if result != nil {
		// else unmarshall to the result type specified by caller
		err := xml.Unmarshal(body, &result)
		if err != nil {
			return err
		}
	}
	return nil
}
