package sources

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type FritzBox struct {
	bytesSentLast int
	bytesRecvLast int
}

func FritzAction(action string) (string, error) {
	requ := `<?xml version="1.0" encoding="utf-8"?>
<s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body>
        <u:%s xmlns:u="urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1">
        </u:%s>
    </s:Body>
</s:Envelope>`

	requ = fmt.Sprintf(requ, action, action)

	r, err := http.NewRequest("POST", "http://169.254.1.1:49000/igdupnp/control/WANCommonIFC1", strings.NewReader(requ))

	if err != nil {
		return "", err
	}

	r.Header.Add("soapaction", "urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1#"+action)
	r.Header.Add("content-type", "text/xml")

	client := http.Client{
		Timeout: 500 * time.Millisecond,
	}

	resp, err := client.Do(r)

	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return "", err
	}

	return string(data), nil
}
