import pandas as pd

NSW1 = {
    pd.Timestamp('2022-01-01').date():'A lot of low price "flatter" days. ,days have low spread and low rmse error.,There is bad spread alignment however this is likely due to prices being close in magnitude.,It will be interesting to see how the optimiser performs on this kind of day compared with one of accurate spread.',
    pd.Timestamp('2024-03-01').date():'The majority of days are flat.,Within this cohort of flatter days a wide varienty of price accuracies and spreads are present.,This Month allows testing on a variety of flat days and.',
    pd.Timestamp('2025-06-01').date():'Features flat days with moderate to high comparitive spread, accurate spread alignment and high RMSE errors.,The optimiser should perform will on these days., ,This month also features Variable and Volitile days with poor RMSE but good spread accuracy.,Overall a good month for testing high spread high spread alignment.',
    pd.Timestamp('2024-10-01').date():'Flatter days with high spread (compared to other days).,Very bad P5 and PD rmse but ok spread alignment.',
    pd.Timestamp('2022-04-01').date():'Majority Variable days with low spread and high price accuracy.,This month also saw poor spread alignment due to extended periods of moderately high prices.,It will be interesting to see how the optimiser handles these days.',
    pd.Timestamp('2022-07-01').date():'Majority Variable days with high spread and low price accuracy.,This month also saw mixed spread alignment due to extended periods of moderately high prices.,It will be interesting to see how the optimiser handles these days.',
    pd.Timestamp('2024-06-01').date():'Mixed bag of High spread flat and var days.,Also a good mix of spread accuracy to test how the optimiser performs when spread is accurate/inaccurate.',
    pd.Timestamp('2023-05-01').date():'01/05 looks interesting.,Also a lot of high spread vol days.,Overall a good mix of Var days with accurate spread and volitile days with high spread and low spread accuracy.',
    pd.Timestamp('2022-06-01').date():'A wild month with very inaccurate days.,Interesting how the opti would perform.,Looks like the price got capped too (2022/06/14)?',
    pd.Timestamp('2024-11-01').date():'A lot of High spread Volitile days with good and some bad spread alignmnent.,Will be good to test performance on vol days with good and bad spread alignment.',
}