SELECT one."Id", "TickerId", "Date","Time","Open", "High", "Low", "Close", "Volume"
	FROM public."TDAmeritradeMinutePrice" as one inner join public."Ticker" as two
	on one."TickerId" = two."Id" where two."Symbol" = 'GOOG' and one."Date" >= '2022-05-17'
	