package authorizers

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/tracing"
)

type AuthorizerCallbackFunc func(tenantID, resource string) (autorest.Authorizer, error)

type AuthorizerCallback struct {
	sender   autorest.Sender
	callback AuthorizerCallbackFunc
}

func NewAuthorizerCallback(sender autorest.Sender, callback AuthorizerCallbackFunc) *AuthorizerCallback {
	if sender == nil {
		sender = &http.Client{Transport: tracing.Transport}
	}
	return &AuthorizerCallback{sender: sender, callback: callback}
}

func (bacb *AuthorizerCallback) WithAuthorization() autorest.PrepareDecorator {
	return func(p autorest.Preparer) autorest.Preparer {
		return autorest.PreparerFunc(func(r *http.Request) (*http.Request, error) {
			r, err := p.Prepare(r)
			if err == nil {
				rCopy := *r
				rCopy.Body = nil
				rCopy.GetBody = nil
				rCopy.ContentLength = 0

				resp, err := bacb.sender.Do(&rCopy)
				if err == nil && resp.StatusCode == 401 {
					defer resp.Body.Close()

					authHeader := resp.Header.Get("Www-Authenticate")
					if len(authHeader) != 0 && strings.Index(authHeader, "Bearer") > -1 {
						bc, err := newChallenge(resp)
						if err != nil {
							return r, err
						}
						if bacb.callback != nil {
							ba, err := bacb.callback(bc.values["tenantID"], bc.values["resource"])
							if err != nil {
								return r, err
							}
							return autorest.Prepare(r, ba.WithAuthorization())
						}
					}
				}
			}
			return r, err
		})
	}
}

type bearerChallenge struct {
	values map[string]string
}

func newChallenge(resp *http.Response) (bc bearerChallenge, err error) {
	challenge := strings.TrimSpace(resp.Header.Get("Www-Authenticate"))
	trimmedChallenge := challenge[len("Bearer")+1:]

	// challenge is a set of key=value pairs that are comma delimited
	pairs := strings.Split(trimmedChallenge, ",")
	if len(pairs) < 1 {
		err = fmt.Errorf("challenge '%s' contains no pairs", challenge)
		return bc, err
	}

	bc.values = make(map[string]string)
	for i := range pairs {
		trimmedPair := strings.TrimSpace(pairs[i])
		pair := strings.Split(trimmedPair, "=")
		if len(pair) == 2 {
			// remove the enclosing quotes
			key := strings.Trim(pair[0], "\"")
			value := strings.Trim(pair[1], "\"")

			switch key {
			case "authorization", "authorization_uri":
				// strip the tenant ID from the authorization URL
				asURL, err := url.Parse(value)
				if err != nil {
					return bc, err
				}
				bc.values["tenantID"] = asURL.Path[1:]
			default:
				bc.values[key] = value
			}
		}
	}

	return bc, err
}
