import requests

url = "https://quotient.p.rapidapi.com/equity/intraday"

querystring = {"symbol":"AAPL","interval":"1","from":"2020-04-21 10:00","to":"2020-04-21 10:30","adjust":"false"}

headers = {
	"X-RapidAPI-Key": "534a6284fcmsh5e52f6b91366755p14d8b7jsn21b0e593b087",
	"X-RapidAPI-Host": "quotient.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)