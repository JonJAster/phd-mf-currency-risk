using Revise
using BenchmarkTools
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    fund_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))
    factor_data = loadarrow(joinpath(DIRS.combo.factors, "factors.arrow"))
    rf_data = loadarrow(joinpath(DIRS.eq.refined, "rf.arrow"))

    mkt_data = factor_data[factor_data.factor .== "mkt" .&& factor_data.source_id .== "ff_usa", :]
    
    us_funds = filter_fundids(x->investment_target_is(x, :usa), fund_data)
    us_rf = innerjoin(us_funds, rf_data, on=:date)
    us_rf.gross_ret = us_rf.ex_ret .+ us_rf.rf
    
    dropmissing!(us_rf, [:gross_ret, :costs])
    us_rf.net_ret = (1 .+ us_rf.gross_ret)./(1 .+ us_rf.costs) .- 1
    
    select!(us_rf, [:fundid, :date, :gross_ret, :net_ret])
    
    rename!(mkt_data, :ret => :mkt)
    mkt_rf = innerjoin(mkt_data, rf_data, on=:date)
    mkt_rf.gross_mkt = round.(mkt_rf.mkt .+ mkt_rf.rf, digits=4)
    select!(mkt_rf, [:date, :gross_mkt])

    function agg_to_annual(data)
        "fundid" in names(data) ? group_cols = [:fundid, :year] : group_cols = [:year]
        agg_cols = setdiff(propertynames(data), [:fundid, :date])

        data.year = Dates.year.(data.date)

        annual_data = combine(
            groupby(data, group_cols),
            agg_cols .=> (x->(prod((1).+x).-1))
        )
        
        rename!(annual_data, [group_cols...; agg_cols...])
        select!(annual_data, [group_cols...; agg_cols...])

        return annual_data
    end

    us_rf_a = agg_to_annual(us_rf)
    mkt_rf_a = agg_to_annual(mkt_rf)


    data = innerjoin(us_rf_a, mkt_rf_a, on=:year)

    fund_tenure = combine(
        groupby(us_rf, :fundid),
        :year => length => :tenure
    )

    sort!(fund_tenure, :tenure, rev=true)
    test_funds = fund_tenure.fundid[1:10]

    for fundid in test_funds # fundid = test_funds[1]
        fund_data = data[data.fundid .== fundid, :]

        bestfit = lm(@formula(net_ret ~ gross_ret), fund_data)

        scatter(fund_data.year, fund_data.net_ret, label=fundid)
end