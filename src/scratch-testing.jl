using Arrow
using CSV
using DataFrames

function test()
    df = Arrow.Table("data/equities/factor-series/unhedged_global_mkt.arrow") |> DataFrame
    sort(df, [:currency, :date])
    display(df_csv[1:20,[1,2,3,320,321,322,323,324,325]])
    println(df)

    println(names(df)[4])
end
