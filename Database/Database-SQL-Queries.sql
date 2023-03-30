SELECT count("Id") FROM public."TDAmeritradeMinutePrice";
	
SELECT "Id", "TickerId", "Date", "Open", "High", "Low", "Close", "Volume"
	FROM public."TDAmeritradeDailyPrice" order by "Id" desc Limit 5; 
	
SELECT "Id", "TickerId", "Date", "Time", "Open", "High", "Low", "Close", "Volume"
	FROM public."TDAmeritradeMinutePrice" order by "Id" desc Limit 5; 
	
SELECT "Id" from public."Ticker" where "Symbol" = 'GOOG'

---- Indexing ----

create index TDAmeritradeMinutePriceTickerId on public."TDAmeritradeMinutePrice"("TickerId") 

create index TDAmeritradeDailyPriceTickerId on public."TDAmeritradeDailyPrice"("TickerId") 

create index TickerSymbol on public."Ticker"("Symbol")

create index CNNFearAndGreeddate on public."CNNFearAndGreed"("date")

create index StocktwitsPostTicker on public."StocktwitsPost"("Ticker")

create index StocktwitsSentimentTicker on public."StocktwitsSentiment"("Ticker")

create index TradingEconomicsYear on public."TradingEconomics"("Year")

create index CboeOptionsdate on public."CboeOptions"("date")
create index CboeOptionsroot on public."CboeOptions"("root")

create index ShortSqueezeRecordDate on public."ShortSqueeze"("Record Date")



