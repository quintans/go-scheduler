package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
)

// ShellJob is the shell command Job, implements the scheduler.Job interface.
// Consider the runtime.GOOS when sending the shell command to execute.
type ShellJob struct {
	slug string
	Cmd  string
}

// NewShellJob returns a new ShellJob.
func NewShellJob(slug string, cmd string) *ShellJob {
	return &ShellJob{slug, cmd}
}

// Description returns a ShellJob description.
func (sh *ShellJob) Description() string {
	return fmt.Sprintf("ShellJob: %s.", sh.Cmd)
}

func (sh *ShellJob) Slug() string {
	return sh.slug
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (sh *ShellJob) Execute(_ context.Context) (string, error) {
	out, err := exec.Command("sh", "-c", sh.Cmd).Output()
	if err != nil {
		return "", err
	}
	return string(out[:]), nil
}

// CurlJob is the curl command Job, implements the scheduler.Job interface.
type CurlJob struct {
	slug          string
	RequestMethod string
	URL           string
	Body          string
	request       *http.Request
}

// NewCurlJob returns a new CurlJob.
func NewCurlJob(
	slug string,
	method string,
	url string,
	body string,
	headers map[string]string,
) (*CurlJob, error) {
	_body := bytes.NewBuffer([]byte(body))
	req, err := http.NewRequest(method, url, _body)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return &CurlJob{
		slug:          slug,
		RequestMethod: method,
		URL:           url,
		Body:          body,
		request:       req,
	}, nil
}

// Description returns a CurlJob description.
func (cu *CurlJob) Description() string {
	return fmt.Sprintf("CurlJob: %s %s %s", cu.RequestMethod, cu.URL, cu.Body)
}

func (cu *CurlJob) Slug() string {
	return cu.slug
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (cu *CurlJob) Execute(_ context.Context) (string, error) {
	client := &http.Client{}
	res, err := client.Do(cu.request)
	if err != nil {
		return "", err
	}

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	result := fmt.Sprintf("%d\n%s", res.StatusCode, string(body))
	if res.StatusCode >= 200 && res.StatusCode < 400 {
		return result, nil
	}

	return "", errors.New(result)
}
