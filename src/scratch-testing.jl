using Arrow
using CSV
using DataFrames

function test()
    df = CSV.read("data/mutual-funds/post-processing/usd-rets_na-int_eq-strict_targets_age-filtered/initialised/mf_usa.csv", DataFrame)
    df_unfiltered = CSV.read("data/mutual-funds/post-processing/usd-rets_na-int_eq-strict_targets/initialised/mf_usa.csv", DataFrame)
    df = dropmissing(df, [:ret_gross_m, :fund_flow])
    df_unfiltered = dropmissing(df_unfiltered, [:ret_gross_m, :fund_flow])

    obs_filt = size(df, 1)
    obs_all = size(df_unfiltered, 1)

    funds_filt = length(unique(df.fundid))
    funds_all = length(unique(df_unfiltered.fundid))

    println("nonmissing observations for all age funds: $obs_all")
    println("nonmissing observations for age-filtered funds: $obs_filt")
    println()
    println("unique funds without filtering: $funds_all")
    println("unique age-filtered funds: $funds_filt")
    println()

    println(
        "obs filtered out: $(obs_all - obs_filt) "*
        "($(round(100 * (obs_all - obs_filt) / obs_all, digits=2))%)"
    )

    println(
        "funds filtered out: $(funds_all - funds_filt) "*
        "($(round(100 * (funds_all - funds_filt) / funds_all, digits=2))%)"
    )
end
